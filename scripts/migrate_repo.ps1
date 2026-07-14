<#
.SYNOPSIS
  Rewrites this repo's full commit history into an isolated, disposable clone
  -- new author/committer identity, public-username/URL scrub across every
  historical commit, a few files stripped from history entirely -- then
  pushes the result to a new branch in the org's git remote.

.DESCRIPTION
  History IS preserved: every commit on the current branch is kept, just
  rewritten (new hashes, since identity/content changed -- unavoidable).
  This differs from a clean-drop/squash approach, which would collapse
  everything into one commit; that was rejected because losing per-commit
  history was not acceptable here.

  The source repository is never rewritten in place. This script clones it
  (`git clone --no-local`) into a disposable temp directory and does all
  rewriting there, via `git filter-repo` -- installed via pip if missing.
  If git-filter-repo genuinely cannot be installed, this falls back to
  `git filter-branch` for identity + path removal, but that fallback CANNOT
  scrub file content/commit messages inside historical blobs -- only the
  final (HEAD) tree is guaranteed clean in that path. You will be asked to
  explicitly acknowledge that gap before it proceeds.

  Safety properties:
    - Requires all tracked source changes to be committed (untracked files
      are fine -- a clone never includes them).
    - Detached HEAD is rejected; tracked submodules are rejected (this
      script does not attempt to migrate them).
    - Validates the destination remote and confirms the target branch does
      not already exist there, before any rewriting happens.
    - Rejects remote URLs containing embedded HTTP credentials.
    - Verifies, across EVERY commit (not just HEAD): author/committer
      identity is exactly the new identity; no forbidden text remains in
      any historical blob, commit message, or historical filename.
    - Creates a full backup bundle of the untouched source repo first
      (audit/recovery convenience -- store it securely, never upload it).
    - Dry-run push before the final typed confirmation. Never force-pushes.

.NOTES
  Run from the source repository root. This file may remain untracked; it
  is explicitly stripped from the migrated history if it were ever tracked.

  No personal identifiers (old GitHub username, old email) are hardcoded
  in this script -- they're collected interactively below, so the file
  itself carries no trace of any specific person and is safe to transfer
  through any channel (email, Teams, USB) without editing.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$script:backupPath = $null

# Files to strip from EVERY historical commit (via --invert-paths), not just
# the final tree. Most of these aren't tracked today (defensive no-ops if so
# -- filter-repo simply finds no matches); zamboni_local.db IS tracked and
# has been rewritten many times across history (seeded demo DB), so this is
# load-bearing for it specifically.
$excludedPaths = @(
    'migrate_repo.ps1',
    '.claude/settings.json',
    '.claude/settings.local.json',
    '.env',
    '.env.aws_local',
    '.streamlit/secrets.toml',
    'zamboni_local.db',
    'zamboni_control.db',
    'zamboni_athena_cache.db-wal',
    'zamboni_athena_cache.db-shm'
)

function Write-Section([string]$Text) {
    Write-Host ""
    Write-Host "== $Text ==" -ForegroundColor Cyan
}

function Set-Utf8NoBomContent([string]$Path, [string[]]$Lines) {
    # Windows PowerShell 5.1's `Set-Content -Encoding UTF8` always writes a
    # UTF-8 BOM -- no "UTF8NoBOM" option exists until PowerShell 7+. git
    # filter-repo (Python) reads the mailmap/replace-text files as plain
    # UTF-8 without BOM-stripping in at least one internal code path, so a
    # BOM here leaks a stray U+FEFF character into the parsed "proper name"
    # field -- confirmed via a real end-to-end test run, where it corrupted
    # the rewritten committer identity (author identity was unaffected,
    # since filter-repo's mailmap handling reads the name for that role via
    # a different path). Writing plain UTF-8 with no BOM avoids this.
    $text = ($Lines -join "`n") + "`n"
    [System.IO.File]::WriteAllText($Path, $text, [System.Text.UTF8Encoding]::new($false))
}

function Confirm-Word([string]$Prompt, [string]$Word) {
    $response = Read-Host "$Prompt (type $Word to continue)"
    if ($response -cne $Word) {
        Write-Host "Aborted. No remote changes were made." -ForegroundColor Yellow
        exit 1
    }
}

function Stop-GitFailure([string]$What, [int]$ExitCode) {
    Write-Host "FAILED: $What (exit code $ExitCode)" -ForegroundColor Red
    if ($script:backupPath) {
        Write-Host "Source backup bundle:" -ForegroundColor Yellow
        Write-Host "  $script:backupPath"
    }
    exit 1
}

function Assert-LastExit([string]$What) {
    if ($LASTEXITCODE -ne 0) {
        Stop-GitFailure $What $LASTEXITCODE
    }
}

function Invoke-GitCapture([string[]]$Arguments) {
    # Windows PowerShell converts redirected native stderr into ErrorRecord
    # objects. With ErrorActionPreference=Stop that would terminate before the
    # caller can inspect Git's exit code, so relax it only for this capture.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = @(& git @Arguments 2>&1)
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    return [pscustomobject]@{
        Output   = $output
        ExitCode = $exitCode
    }
}

function Read-Utf8TextFile([string]$Path) {
    $bytes = [System.IO.File]::ReadAllBytes($Path)
    if ([Array]::IndexOf($bytes, [byte]0) -ge 0) {
        return $null
    }

    $hasBom = (
        $bytes.Length -ge 3 -and
        $bytes[0] -eq 0xEF -and
        $bytes[1] -eq 0xBB -and
        $bytes[2] -eq 0xBF
    )
    $offset = if ($hasBom) { 3 } else { 0 }

    try {
        $strictUtf8 = [System.Text.UTF8Encoding]::new($false, $true)
        $text = $strictUtf8.GetString($bytes, $offset, $bytes.Length - $offset)
        return [pscustomobject]@{
            Text   = $text
            HasBom = $hasBom
        }
    }
    catch [System.Text.DecoderFallbackException] {
        return $null
    }
}

function Replace-LiteralIgnoreCase(
    [string]$Text,
    [string]$OldValue,
    [string]$NewValue
) {
    $regex = [System.Text.RegularExpressions.Regex]::new(
        [System.Text.RegularExpressions.Regex]::Escape($OldValue),
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    )
    $count = $regex.Matches($Text).Count
    if ($count -eq 0) {
        return [pscustomobject]@{ Text = $Text; Count = 0 }
    }

    # .NET replacement strings treat '$' specially. Doubling it makes the
    # caller's value literal, including unusual-but-valid URLs containing '$'.
    $literalReplacement = $NewValue.Replace('$', '$$')
    return [pscustomobject]@{
        Text  = $regex.Replace($Text, $literalReplacement)
        Count = $count
    }
}

function Find-ForbiddenReferences(
    [string]$Root,
    [string[]]$Patterns
) {
    $violations = New-Object System.Collections.Generic.List[string]
    $gitRoot = Join-Path $Root '.git'
    $gitPrefix = $gitRoot + [System.IO.Path]::DirectorySeparatorChar
    $files = Get-ChildItem -LiteralPath $Root -Recurse -File -Force |
        Where-Object {
            -not $_.FullName.StartsWith(
                $gitPrefix,
                [System.StringComparison]::OrdinalIgnoreCase
            )
        }

    foreach ($file in $files) {
        $relative = $file.FullName.Substring($Root.Length).TrimStart('\', '/')
        foreach ($pattern in $Patterns) {
            if ($relative.IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                $violations.Add("$relative (path) -> $pattern")
            }
        }

        $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
        $ascii = [System.Text.Encoding]::ASCII.GetString($bytes)
        foreach ($pattern in $Patterns) {
            if ($ascii.IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                $violations.Add("$relative -> $pattern")
            }
        }
    }
    return $violations
}

function Test-HistoryClean([string]$Root, [string[]]$Patterns) {
    # git grep exit codes are inverted from what you'd expect: 1 = no match
    # (good), 0 = match found (bad), anything else = a real error.
    $revs = @(git -C $Root rev-list --all)
    Assert-LastExit "list all revisions for history content scan"
    if ($revs.Count -eq 0) { return @() }

    $arguments = New-Object System.Collections.Generic.List[string]
    $arguments.Add('-C'); $arguments.Add($Root)
    $arguments.Add('grep'); $arguments.Add('-a'); $arguments.Add('-i')
    foreach ($pattern in $Patterns) {
        $arguments.Add('-e'); $arguments.Add($pattern)
    }
    foreach ($rev in $revs) { $arguments.Add($rev) }

    $result = Invoke-GitCapture -Arguments $arguments.ToArray()
    if ($result.ExitCode -eq 1) { return @() }
    if ($result.ExitCode -eq 0) { return $result.Output }
    Stop-GitFailure "history content scan (git grep)" $result.ExitCode
}

function Get-HistoricalPathViolations([string]$Root, [string[]]$Patterns) {
    $paths = @(git -C $Root log --all --name-only --format=) |
        Where-Object { $_ -and $_.Trim() -ne '' } |
        Select-Object -Unique
    Assert-LastExit "list all historical filenames"

    $violations = New-Object System.Collections.Generic.List[string]
    foreach ($path in $paths) {
        foreach ($pattern in $Patterns) {
            if ($path.IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                $violations.Add("$path (historical filename) -> $pattern")
            }
        }
    }
    return $violations
}

function Test-CommitMessagesClean([string]$Root, [string[]]$Patterns) {
    # Test-HistoryClean (git grep <rev>) searches each revision's TREE --
    # file contents at that point in history -- never the commit message
    # itself, which is metadata on the commit object, not part of any blob.
    # Nothing else in this script's verification step reads commit messages
    # either (the identity check only reads %an/%ae/%cn/%ce). That left a
    # real gap: --replace-message scrubs messages during the REWRITE, but
    # nothing independently CONFIRMED it worked, so this script could print
    # "no forbidden references found" while an unscrubbed message still
    # carried the old identity. Concatenates every commit's raw body (%B)
    # into one corpus and does the same fixed-string (not regex),
    # case-insensitive substring search Find-ForbiddenReferences already
    # uses for file content -- deliberately simple, not per-commit
    # attribution, since "something in some message still matches" is
    # already enough to fail closed and prompt a manual
    # `git log --all --grep` to find which commit.
    $allMessages = (git -C $Root log --all --format='%B') -join "`n"
    Assert-LastExit "read commit messages across history"

    $violations = New-Object System.Collections.Generic.List[string]
    foreach ($pattern in $Patterns) {
        if ($allMessages.IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $violations.Add("commit message (some commit) -> $pattern")
        }
    }
    return $violations
}

function Get-AmbiguousPatterns([string[]]$Patterns, [string[]]$NewIdentityValues) {
    # A forbidden (old) pattern that is itself a substring of one of the
    # actual NEW identity values (e.g. new username "Org-Sujith_ace"
    # legitimately containing old username "Sujith_ace") can never be fully
    # verified via plain substring matching -- its continued appearance
    # in the final history is the CORRECT, intended new identity, not proof
    # of a leftover. Every match of such a pattern is ambiguous going
    # forward; anything NOT in this set has no legitimate reason to appear
    # anywhere, so a match of it is unambiguous proof of a real leak.
    $values = @($NewIdentityValues | Where-Object { $_ })
    $ambiguous = New-Object System.Collections.Generic.List[string]
    foreach ($pattern in $Patterns) {
        foreach ($value in $values) {
            if ($value.IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
                $ambiguous.Add($pattern)
                break
            }
        }
    }
    return $ambiguous
}

function Split-Violations([string[]]$Violations, [string[]]$AmbiguousPatterns) {
    # Every violation string produced by Find-ForbiddenReferences /
    # Test-HistoryClean / Get-HistoricalPathViolations /
    # Test-CommitMessagesClean ends with "-> <pattern>" -- use that to sort
    # each violation into "ambiguous" (matched a pattern that's also part of
    # a real new-identity value -- expected, not fatal) or "fatal"
    # (matched a pattern with no legitimate reason to appear -- a real leak).
    $fatal = New-Object System.Collections.Generic.List[string]
    $ambiguousOut = New-Object System.Collections.Generic.List[string]
    foreach ($violation in $Violations) {
        $isAmbiguous = $false
        foreach ($pattern in $AmbiguousPatterns) {
            if ($violation.EndsWith("-> $pattern", [System.StringComparison]::OrdinalIgnoreCase)) {
                $isAmbiguous = $true
                break
            }
        }
        if ($isAmbiguous) { $ambiguousOut.Add($violation) } else { $fatal.Add($violation) }
    }
    return [pscustomobject]@{ Fatal = $fatal; Ambiguous = $ambiguousOut }
}

# ---------------------------------------------------------------------------
# 0. Source preconditions
# ---------------------------------------------------------------------------
Write-Section "Checking source repository"

$repoRoot = git rev-parse --show-toplevel 2>$null
if ($LASTEXITCODE -ne 0 -or -not $repoRoot) {
    Write-Host "Not inside a Git repository. Aborting." -ForegroundColor Red
    exit 1
}
$repoRoot = (Resolve-Path $repoRoot).Path
Set-Location $repoRoot

$currentBranch = git symbolic-ref --quiet --short HEAD 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($currentBranch)) {
    Write-Host "Detached HEAD is not supported. Check out the source branch to migrate." -ForegroundColor Red
    exit 1
}

git rev-parse --verify HEAD *> $null
Assert-LastExit "verify source HEAD"
$sourceCommit = git rev-parse HEAD

$trackedChanges = @(git status --porcelain --untracked-files=no)
Assert-LastExit "inspect tracked source changes"
if ($trackedChanges.Count -gt 0) {
    Write-Host "Tracked files have uncommitted changes. Commit them before migrating:" -ForegroundColor Red
    $trackedChanges | ForEach-Object { Write-Host "  $_" }
    exit 1
}

$untrackedFiles = @(git ls-files --others --exclude-standard)
Assert-LastExit "inspect untracked source files"

$sourceTree = @(git ls-tree -r --full-tree HEAD)
Assert-LastExit "inspect source tree modes"
$submodules = @($sourceTree | Where-Object { $_ -match '^160000\s' })
if ($submodules.Count -gt 0) {
    Write-Host "Tracked submodules were found; this script does not migrate them:" -ForegroundColor Red
    $submodules | ForEach-Object { Write-Host "  $_" }
    exit 1
}

$sourceCommitCount = [int](git rev-list $currentBranch --count)
Assert-LastExit "count source commits"

Write-Host "Source root    : $repoRoot"
Write-Host "Source branch  : $currentBranch"
Write-Host "Source HEAD    : $sourceCommit"
Write-Host "Commits to move: $sourceCommitCount"
if ($untrackedFiles.Count -gt 0) {
    Write-Host "Untracked files excluded from the migration (a clone never includes them):" -ForegroundColor Yellow
    $untrackedFiles | ForEach-Object { Write-Host "  $_" }
}

# ---------------------------------------------------------------------------
# 1. Collect and validate inputs
# ---------------------------------------------------------------------------
Write-Section "Source identity to scrub"

do {
    $oldUsernamesInput = Read-Host "GitHub username(s) to remove from history (comma-separated if you've used more than one)"
} while ([string]::IsNullOrWhiteSpace($oldUsernamesInput))
$oldUsernames = @(
    $oldUsernamesInput -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
)
foreach ($oldUser in $oldUsernames) {
    if ($oldUser -notmatch '^[A-Za-z0-9._-]+$') {
        Write-Host "GitHub username '$oldUser' may contain only letters, digits, '.', '_' and '-'." -ForegroundColor Red
        exit 1
    }
    # Each entry here becomes a bounded-but-unanchored regex match
    # (line ~734: (?<![A-Za-z0-9_])$escapedOldUser(?![A-Za-z0-9_])) applied
    # to EVERY historical blob and commit message, not just genuine username
    # mentions. A short or purely-numeric entry (a stray "5" from a
    # mis-pasted list, a typo, a copy/paste artifact) matches constants,
    # version numbers, and other innocuous standalone tokens throughout the
    # codebase and silently replaces them too -- confirmed via a real
    # incident where a single-character entry clobbered a safety constant
    # in config/settings.py. Real GitHub usernames are always 1-39 chars,
    # but requiring >= 4 and rejecting purely-numeric entries costs nothing
    # for a legitimate username while closing off the two shapes most
    # likely to be an accidental short/generic token.
    if ($oldUser.Length -lt 4 -or $oldUser -match '^[0-9]+$') {
        Write-Host "GitHub username '$oldUser' is too short or purely numeric to safely scrub -- it would be blanket-replaced everywhere it appears as a standalone token across the ENTIRE history (constants, version numbers, etc.), not just genuine username mentions. Re-run and enter your real GitHub username(s) only." -ForegroundColor Red
        exit 1
    }
}

# Length/numeric checks above catch a stray "5", but not a real English word
# entered by mistake (e.g. "claude", "sonnet" -- confirmed via a real
# incident where both were entered as "old usernames," each blanket-matching
# every standalone mention of those words in every file across all of
# history, not just genuine username references). A full `git grep --all`
# scan is what the (much slower) post-rewrite verification already does --
# too expensive to run here as a pre-check. Counting matches in HEAD alone
# is a fast, reliable proxy instead: HEAD already contains the overwhelming
# majority of surviving content, and a genuine GitHub username realistically
# appears a handful of times (a git remote URL, maybe a couple of
# authorship mentions) while a common word or stray token appears dozens to
# thousands of times.
Write-Section "Sanity-checking scrub patterns against HEAD"
$MAX_SAFE_HEAD_MATCHES = 25
foreach ($oldUser in $oldUsernames) {
    $escapedOldUser = [System.Text.RegularExpressions.Regex]::Escape($oldUser)
    $boundedPattern = "(?<![A-Za-z0-9_])$escapedOldUser(?![A-Za-z0-9_])"
    # -P (PCRE), not -E (POSIX extended) -- confirmed via a live test that -E
    # errors out (exit 128, invalid regex) on the (?<!...)/(?!...) lookaround
    # this bounded pattern needs, since POSIX ERE has no lookaround syntax at
    # all; -P is what makes it behave like a real "no match" instead of a
    # crash.
    $countResult = Invoke-GitCapture -Arguments @('grep', '-a', '-i', '-c', '-P', $boundedPattern, 'HEAD')
    $totalMatches = 0
    if ($countResult.ExitCode -eq 0) {
        foreach ($line in $countResult.Output) {
            if ([string]$line -match ':(\d+)$') { $totalMatches += [int]$Matches[1] }
        }
    }
    elseif ($countResult.ExitCode -ne 1) {
        # 1 == no matches (fine); anything else is a real grep error.
        Stop-GitFailure "sanity-check scrub pattern '$oldUser' against HEAD" $countResult.ExitCode
    }

    if ($totalMatches -gt $MAX_SAFE_HEAD_MATCHES) {
        Write-Host "'$oldUser' matches $totalMatches time(s) in the current HEAD tree alone -- this looks like a common word or generic token, not a real GitHub username." -ForegroundColor Red
        Write-Host "Scrubbing it will blanket-replace every one of those occurrences (and every occurrence across all of history) with the org replacement token, whether or not it's actually a username reference there." -ForegroundColor Red
        Write-Host "First few matches:" -ForegroundColor Yellow
        $sampleResult = Invoke-GitCapture -Arguments @('grep', '-a', '-i', '-n', '-P', $boundedPattern, 'HEAD')
        $sampleResult.Output | Select-Object -First 5 | ForEach-Object { Write-Host "  $_" }
        Write-Host "If '$oldUser' is genuinely your GitHub username and this is a coincidence, confirm below to proceed anyway." -ForegroundColor Yellow
        Confirm-Word "Proceed with scrubbing '$oldUser' despite the high match count" "CONFIRM-SCRUB"
    }
}

do {
    $oldEmail = (Read-Host "Personal email to remove from history").Trim()
    if ($oldEmail -notmatch '^[^\s@]+@[^\s@]+\.[^\s@]+$') {
        Write-Host "Enter a valid email address." -ForegroundColor Yellow
        $oldEmail = ''
    }
} while ([string]::IsNullOrWhiteSpace($oldEmail))

# 'noreply@github.com' and '__ZAMBONI_ORG_' are generic (GitHub's own
# web-merge committer address, and this script's own internal token
# prefix) -- not personal to whoever is running this, so they're the only
# entries not derived from the prompts above.
$forbiddenPatterns = @($oldUsernames) + @($oldEmail) + @('noreply@github.com', '__ZAMBONI_ORG_') +
    @($oldUsernames | ForEach-Object { "github.com/$_" })

Write-Section "Org identity"

$newName = (Read-Host "New author/committer name (e.g. Jane Doe)").Trim()
if ([string]::IsNullOrWhiteSpace($newName)) {
    Write-Host "A name is required." -ForegroundColor Red
    exit 1
}

do {
    $newEmail = (Read-Host "New author/committer email (your org email)").Trim()
    if ($newEmail -notmatch '^[^\s@]+@[^\s@]+\.[^\s@]+$') {
        Write-Host "Enter a valid email address." -ForegroundColor Yellow
        $newEmail = ''
    }
    elseif ($newEmail.Contains('\')) {
        # Same reasoning as the org remote URL's backslash check below: this
        # value becomes the replacement side of a regex:-prefixed
        # --replace-text/--replace-message rule (see the "Phase 2" fix
        # below), and a backslash in a Python re.sub() replacement string is
        # a backreference/escape sequence, not a literal character.
        Write-Host "Email contains a backslash -- unsafe as a regex replacement value." -ForegroundColor Yellow
        $newEmail = ''
    }
} while ([string]::IsNullOrWhiteSpace($newEmail))

$usernameReplacement = Read-Host "Replacement for public usernames [your-org-username]"
if ([string]::IsNullOrWhiteSpace($usernameReplacement)) { $usernameReplacement = 'your-org-username' }
$usernameReplacement = $usernameReplacement.Trim()
if ($usernameReplacement -notmatch '^[A-Za-z0-9._-]+$') {
    Write-Host "Username replacement may contain only letters, digits, '.', '_' and '-'." -ForegroundColor Red
    exit 1
}

Write-Section "Org destination"

do {
    $orgRemoteUrl = (Read-Host "New org Git remote URL (full clone URL)").Trim()
} while ([string]::IsNullOrWhiteSpace($orgRemoteUrl))

if ($orgRemoteUrl -match '^https?://[^/\s]+@') {
    Write-Host "Do not embed a username, PAT, or password in the remote URL. Use a credential helper or SSH." -ForegroundColor Red
    exit 1
}
if ($orgRemoteUrl.Contains('\')) {
    Write-Host "Remote URL contains a backslash -- not a valid git URL, and unsafe as a regex replacement value." -ForegroundColor Red
    exit 1
}

$remoteName = Read-Host "Local name for the org remote [origin]"
if ([string]::IsNullOrWhiteSpace($remoteName)) { $remoteName = 'origin' }
$remoteName = $remoteName.Trim()
if ($remoteName -notmatch '^(?!-)[A-Za-z0-9._-]+$') {
    Write-Host "Remote name may contain only letters, digits, '.', '_' and '-', and cannot start with '-'." -ForegroundColor Red
    exit 1
}

$targetBranch = Read-Host "New org branch [new-phase1]"
if ([string]::IsNullOrWhiteSpace($targetBranch)) { $targetBranch = 'new-phase1' }
$targetBranch = $targetBranch.Trim()
git check-ref-format --branch $targetBranch *> $null
Assert-LastExit "validate target branch name"

# The rewritten history has its own brand-new root commit (filter-repo/
# filter-branch never share an ancestor with anything already on the org
# remote) -- pushing it as a bare new branch and then opening a PR against
# an existing branch that already has content produces "unrelated
# histories," which GitHub's compare view surfaces as no diff at all (reads
# like "I can't see any change," not an error) -- confirmed via a real
# incident. If the org branch below already has real content, this script
# rebuilds the rewritten commits as patches on top of it (git format-patch +
# git am) instead of pushing a disconnected branch, so the result shares a
# real ancestor and a PR against it shows a normal diff.
$orgBaseBranch = Read-Host "Org branch to base this on / open the PR against, if it already has content -- leave blank if the org repo is brand new/empty"
$orgBaseBranch = $orgBaseBranch.Trim()

$remoteWithoutGitSuffix = ($orgRemoteUrl -replace '(?i)\.git$', '')
$defaultPublicUrl = $remoteWithoutGitSuffix
if ($remoteWithoutGitSuffix -match '^git@([^:]+):(.+)$') {
    $defaultPublicUrl = "https://$($Matches[1])/$($Matches[2])"
}
elseif ($remoteWithoutGitSuffix -match '^ssh://(?:[^@/]+@)?([^/:]+)(?::\d+)?/(.+)$') {
    $defaultPublicUrl = "https://$($Matches[1])/$($Matches[2])"
}

$publicRepoUrl = Read-Host "Public repository URL used in docs [$defaultPublicUrl]"
if ([string]::IsNullOrWhiteSpace($publicRepoUrl)) { $publicRepoUrl = $defaultPublicUrl }
$publicRepoUrl = $publicRepoUrl.Trim().TrimEnd('/')
if ($publicRepoUrl -match '^https?://[^/\s]+@') {
    Write-Host "Do not embed credentials in the public repository URL." -ForegroundColor Red
    exit 1
}
if ($publicRepoUrl.Contains('\')) {
    Write-Host "Public repository URL contains a backslash -- unsafe as a regex replacement value." -ForegroundColor Red
    exit 1
}

$deliverableInputs = [ordered]@{
    'author/committer name' = $newName
    'author/committer email' = $newEmail
    'username replacement' = $usernameReplacement
    'org remote URL' = $orgRemoteUrl
    'public repository URL' = $publicRepoUrl
    'remote name' = $remoteName
    'target branch' = $targetBranch
}
# Disabled: this pre-flight substring check against $forbiddenPatterns
# false-positived on legitimate new-identity values that happen to share a
# word with the old username/email (e.g. an org name containing the same
# name the old username was derived from). The real safety net -- scanning
# the REWRITTEN HISTORY CONTENT for leftover old identity -- is untouched
# below (Find-ForbiddenReferences / Test-HistoryClean /
# Get-HistoricalPathViolations / Test-CommitMessagesClean); this only
# disabled the input-typo guard on the 7 new-identity prompts themselves.
#
# foreach ($entry in $deliverableInputs.GetEnumerator()) {
#     foreach ($pattern in $forbiddenPatterns) {
#         if ([string]$entry.Value -and
#             ([string]$entry.Value).IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
#             Write-Host "The $($entry.Key) still contains forbidden legacy value '$pattern'." -ForegroundColor Red
#             exit 1
#         }
#     }
# }

# Computed once, here, right after every new-identity value is final --
# reused by both post-rewrite verification passes below (fallback tree-only
# check and the full-history check) so a pattern that's legitimately part of
# the chosen new identity (e.g. new username "Org-Sujith_ace" containing old
# username "Sujith_ace") is never treated as a fatal leak in either path.
$ambiguousPatterns = @(Get-AmbiguousPatterns $forbiddenPatterns @($newName, $newEmail, $usernameReplacement, $orgRemoteUrl, $publicRepoUrl))
if ($ambiguousPatterns.Count -gt 0) {
    Write-Host "Note: your new identity value(s) legitimately contain the following old pattern(s) as a substring -- their continued appearance in the rewritten history will be treated as expected, not a leak:" -ForegroundColor Yellow
    $ambiguousPatterns | ForEach-Object { Write-Host "  $_" }
}

# ---------------------------------------------------------------------------
# 2. Validate destination before creating or changing anything
# ---------------------------------------------------------------------------
Write-Section "Validating org destination"

$remoteResult = Invoke-GitCapture -Arguments @('ls-remote', '--', $orgRemoteUrl)
if ($remoteResult.ExitCode -ne 0) {
    Write-Host "Cannot read the org remote. Verify the URL, network access, and credentials:" -ForegroundColor Red
    $remoteResult.Output | ForEach-Object { Write-Host "  $_" }
    exit 1
}
Write-Host "Remote is reachable." -ForegroundColor Green

$expectedRemoteRef = "refs/heads/$targetBranch"
$targetResult = Invoke-GitCapture -Arguments @('ls-remote', '--heads', '--', $orgRemoteUrl, $expectedRemoteRef)
if ($targetResult.ExitCode -ne 0) {
    Write-Host "Could not check whether the destination branch exists." -ForegroundColor Red
    $targetResult.Output | ForEach-Object { Write-Host "  $_" }
    exit 1
}
$targetMatches = @(
    $targetResult.Output | Where-Object {
        $parts = [string]$_ -split "`t", 2
        $parts.Count -eq 2 -and $parts[1] -ceq $expectedRemoteRef
    }
)
if ($targetMatches.Count -gt 0) {
    Write-Host "Destination branch '$targetBranch' already exists. Refusing to overwrite it." -ForegroundColor Red
    Write-Host "Choose a new branch or handle the existing branch through the org's normal review process."
    exit 1
}
Write-Host "Destination branch '$targetBranch' does not exist." -ForegroundColor Green

# ---------------------------------------------------------------------------
# 3. Rewrite-tool detection (before any backup/clone work is done)
# ---------------------------------------------------------------------------
Write-Section "Checking for git-filter-repo"

git filter-repo --version *> $null
$haveFilterRepo = ($LASTEXITCODE -eq 0)

if (-not $haveFilterRepo) {
    Write-Host "git-filter-repo not found -- attempting 'pip install git-filter-repo'..."
    pip install git-filter-repo *> $null
    if ($LASTEXITCODE -eq 0) {
        git filter-repo --version *> $null
        $haveFilterRepo = ($LASTEXITCODE -eq 0)
    }
}

if ($haveFilterRepo) {
    Write-Host "git-filter-repo is available -- full history (content + identity + paths) will be rewritten." -ForegroundColor Green
}
else {
    Write-Host "git-filter-repo is unavailable (no pip / no internet?)." -ForegroundColor Yellow
    Write-Host "Falling back to 'git filter-branch', which can rewrite author/committer" -ForegroundColor Yellow
    Write-Host "identity and remove excluded paths across ALL history, but CANNOT scrub" -ForegroundColor Yellow
    Write-Host "file content or commit messages inside historical blob versions -- only" -ForegroundColor Yellow
    Write-Host "the final (HEAD) tree will be guaranteed clean in that path. Older commits" -ForegroundColor Yellow
    Write-Host "may still contain the old username/URL wherever they appeared at that point" -ForegroundColor Yellow
    Write-Host "in history." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "For full protection, install git-filter-repo yourself" -ForegroundColor Yellow
    Write-Host "(pip install git-filter-repo) and re-run this script." -ForegroundColor Yellow
    Confirm-Word "Continue anyway with reduced (HEAD-only) content-scrub guarantees" "ACKNOWLEDGE"
}

# ---------------------------------------------------------------------------
# 4. Summary, confirmation, and source backup
# ---------------------------------------------------------------------------
Write-Section "Migration summary"

Write-Host "Source HEAD           : $sourceCommit ($currentBranch)"
Write-Host "Commits to migrate     : $sourceCommitCount"
Write-Host "Org identity           : $newName <$newEmail>"
Write-Host "Username replacement   : $usernameReplacement"
Write-Host "Org remote             : $orgRemoteUrl"
Write-Host "Public repository URL  : $publicRepoUrl"
Write-Host "New branch             : $targetBranch"
Write-Host "Base branch (for PR)   : $(if ($orgBaseBranch) { $orgBaseBranch } else { '(none -- pushed as a standalone new branch)' })"
Write-Host "History                : PRESERVED (every commit rewritten, new hashes)"
Write-Host "Rewrite engine         : $(if ($haveFilterRepo) { 'git-filter-repo (full)' } else { 'git filter-branch (reduced guarantees)' })"
Write-Host "Source repository      : remains untouched"
Confirm-Word "Proceed with the isolated history rewrite" "MIGRATE"

Write-Section "Creating source backup bundle"

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$migrationId = "$timestamp-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$script:backupPath = Join-Path (Split-Path $repoRoot -Parent) "zamboni-pre-migration-$migrationId.bundle"
git bundle create $script:backupPath --all
Assert-LastExit "create source backup bundle"
Write-Host "Backup created: $script:backupPath" -ForegroundColor Green
Write-Host "This bundle contains the OLD personal history. Store it securely and never upload it to the org." -ForegroundColor Yellow

# ---------------------------------------------------------------------------
# 5. Clone the source branch into an isolated, disposable directory
# ---------------------------------------------------------------------------
Write-Section "Cloning source branch into an isolated directory"

$migrationRoot = Join-Path ([System.IO.Path]::GetTempPath()) "zamboni-org-migration-$migrationId"
$isolatedRepo = Join-Path $migrationRoot 'repo'
if (Test-Path -LiteralPath $migrationRoot) {
    Write-Host "Unique migration directory unexpectedly already exists: $migrationRoot" -ForegroundColor Red
    exit 1
}
New-Item -ItemType Directory -Path $migrationRoot *> $null

# --no-local forces a full object copy instead of a hardlinked/local-optimized
# clone -- git-filter-repo's own docs recommend this when rewriting a clone
# made from a local path, so the source repo's object store is never at risk.
git clone --no-local --single-branch --branch $currentBranch -- $repoRoot $isolatedRepo
Assert-LastExit "clone source branch into isolated directory"

# The clone's 'origin' remote-tracking ref (refs/remotes/origin/<branch>)
# still points at the ORIGINAL, unrewritten commit after filter-branch --all
# (confirmed empirically -- filter-branch does not reliably update it), which
# would leak unrewritten content back into every --all-scoped check/push
# below. Removing the remote (and its tracking refs) before any rewrite
# starts closes that gap for both rewrite paths.
git -C $isolatedRepo remote remove origin
Assert-LastExit "remove clone's origin remote before rewriting"

$emptyHooksDir = Join-Path $migrationRoot 'empty-hooks'
New-Item -ItemType Directory -Path $emptyHooksDir *> $null
git -C $isolatedRepo config core.hooksPath $emptyHooksDir
Assert-LastExit "disable hooks in isolated repository"
git -C $isolatedRepo config core.autocrlf false
Assert-LastExit "disable environment-specific line-ending conversion"
git -C $isolatedRepo config commit.gpgSign false
Assert-LastExit "disable environment-specific commit signing"

# ---------------------------------------------------------------------------
# 6. Rewrite: identity + path removal + (filter-repo only) content scrub,
#    across every commit
# ---------------------------------------------------------------------------
Write-Section "Rewriting history"

$mailmapFile = Join-Path $migrationRoot 'mailmap.txt'
Set-Utf8NoBomContent $mailmapFile @(
    "$newName <$newEmail> <$oldEmail>"
    "$newName <$newEmail> <noreply@github.com>"
)

if ($haveFilterRepo) {
    # Two-phase token indirection (old pattern -> unique token -> final
    # value) -- NOT old-pattern -> final-value directly. filter-repo applies
    # every line in this file sequentially to the running text, so a rule's
    # OUTPUT is visible to every rule listed after it. If any "new" value
    # itself contains one of the "old" patterns as a substring -- entirely
    # plausible, e.g. a new org username that keeps a personal handle as a
    # suffix ("Org-Sujith_ace" containing old username "Sujith_ace") -- an
    # earlier rule inserting that new value creates fresh text that a later
    # old-pattern rule then partially re-matches and re-replaces, corrupting
    # the result (e.g. "Org-Org-Sujith_ace"). Confirmed via a real run that
    # hit exactly this. Routing every old pattern to a unique,
    # nothing-like-real-data token first, with ALL token->final-value
    # substitutions deferred to their own rules at the very end of the file,
    # makes that impossible: no old-pattern rule ever runs after a final
    # value has been written into the text. This mirrors the
    # $tokenRules/$tokenValues two-phase approach the filter-branch fallback
    # below already uses (that path was never vulnerable to this, since its
    # two phases were already separate loops) -- this brings the primary
    # path in line with it.
    $replaceTextFile = Join-Path $migrationRoot 'replace-text.txt'
    $replaceTextLines = New-Object System.Collections.Generic.List[string]

    $tokenRemoteGit     = '__ZAMBONI_ORG_REMOTE_GIT__'
    $tokenRemoteDisplay = '__ZAMBONI_ORG_REMOTE_DISPLAY__'
    $tokenEmail         = '__ZAMBONI_ORG_EMAIL__'
    $tokenUsername      = '__ZAMBONI_ORG_USERNAME__'

    foreach ($oldUser in $oldUsernames) {
        # Escaped: the input validation above allows '.' (a regex
        # metacharacter -- real GitHub usernames can't actually contain one,
        # but this script's own validation is more permissive than that), so
        # an unescaped $oldUser here could match more than the literal
        # username wherever a '.' appears in place of any character.
        $escapedOldUser = [System.Text.RegularExpressions.Regex]::Escape($oldUser)
        $replaceTextLines.Add("regex:(?i)https://github\.com/$escapedOldUser/zamboni\.git==>$tokenRemoteGit")
        $replaceTextLines.Add("regex:(?i)git@github\.com:$escapedOldUser/zamboni\.git==>$tokenRemoteGit")
        $replaceTextLines.Add("regex:(?i)https://github\.com/$escapedOldUser/zamboni==>$tokenRemoteDisplay")
        $replaceTextLines.Add("regex:(?i)github\.com/$escapedOldUser/zamboni==>$tokenRemoteDisplay")
    }
    $escapedOldEmail = [System.Text.RegularExpressions.Regex]::Escape($oldEmail)
    $replaceTextLines.Add("regex:(?i)$escapedOldEmail==>$tokenEmail")
    $replaceTextLines.Add("regex:(?i)noreply@github\.com==>$tokenEmail")
    foreach ($oldUser in $oldUsernames) {
        # This rule has no surrounding literal context (unlike the four
        # github.com/... rules above) to naturally bound the match, so a
        # short/common username could otherwise over-match as a substring
        # of unrelated words anywhere in history (e.g. username "jo" inside
        # "enjoy"). Escaped + bounded with lookaround on both sides -- but
        # ONLY against letters/digits/underscore, deliberately excluding
        # '.' and '-' from the boundary class. Confirmed via a live test
        # run against a throwaway repo: a commit message reading "...thanks
        # faketestuser123-helper for the review" is a genuine mention of
        # username "faketestuser123", not an unrelated word it happens to
        # be a substring of -- an earlier version of this rule that also
        # excluded '.'/'-' from matching *adjacent* to the username (i.e.
        # treated them as non-boundary "word" characters, matching this
        # script's own username-validation charset) left that occurrence
        # unscrubbed, only caught afterward by Test-CommitMessagesClean's
        # independent, unbounded verification scan.
        $escapedOldUser = [System.Text.RegularExpressions.Regex]::Escape($oldUser)
        $replaceTextLines.Add("regex:(?i)(?<![A-Za-z0-9_])$escapedOldUser(?![A-Za-z0-9_])==>$tokenUsername")
    }

    # Phase 2 -- tokens to final values, deliberately listed last so no
    # old-pattern rule above can ever run against an already-final value.
    #
    # MUST be regex:-prefixed, not plain literal rules -- confirmed via a
    # real incident plus direct isolated testing against git filter-repo:
    # it internally applies every PLAIN (non-regex:) --replace-text rule in
    # one pass, THEN every regex: rule in a second pass, regardless of the
    # order rules appear in the file. Phase 1 above is regex: (old pattern
    # -> token); a plain-literal phase 2 (token -> final value) therefore
    # ran in the FIRST pass, before phase 1's regex pass ever produced the
    # token text to match against -- so the token was left in the final
    # output, unresolved, in every historical commit. Making phase 2
    # regex: too puts both phases in the SAME (second) pass, applied in
    # file order, so phase 2 correctly sees phase 1's freshly-inserted
    # tokens. The replacement (right-hand) side goes through Python's
    # re.sub(), where a backslash is a backreference/escape sequence, not
    # a literal character -- $orgRemoteUrl/$publicRepoUrl/$usernameReplacement
    # are already validated elsewhere to reject backslashes, and $newEmail
    # now is too (see its prompt above), so Regex.Escape() is only needed
    # on the search (left-hand) side here.
    $escapedTokenRemoteGit = [System.Text.RegularExpressions.Regex]::Escape($tokenRemoteGit)
    $escapedTokenRemoteDisplay = [System.Text.RegularExpressions.Regex]::Escape($tokenRemoteDisplay)
    $escapedTokenEmail = [System.Text.RegularExpressions.Regex]::Escape($tokenEmail)
    $escapedTokenUsername = [System.Text.RegularExpressions.Regex]::Escape($tokenUsername)
    $replaceTextLines.Add("regex:$escapedTokenRemoteGit==>$orgRemoteUrl")
    $replaceTextLines.Add("regex:$escapedTokenRemoteDisplay==>$publicRepoUrl")
    $replaceTextLines.Add("regex:$escapedTokenEmail==>$newEmail")
    $replaceTextLines.Add("regex:$escapedTokenUsername==>$usernameReplacement")

    Set-Utf8NoBomContent $replaceTextFile $replaceTextLines

    # A SEPARATE file for --replace-message only -- deliberately not folded
    # into $replaceTextFile, which also feeds --replace-text (file content).
    # "Claude Sonnet 5" -> "Claude" is a commit-message display-name
    # simplification (the Co-Authored-By trailer this tool writes), not an
    # old-identity leak: applying it to file content too would also rewrite
    # the literal model-id string ("claude-sonnet-5") wherever it's used as
    # a real technical reference (e.g. config/model selection), which is a
    # different, unrelated string this script has no business touching.
    $replaceMessageFile = Join-Path $migrationRoot 'replace-message.txt'
    $replaceMessageLines = New-Object System.Collections.Generic.List[string]
    $replaceMessageLines.AddRange($replaceTextLines)
    $replaceMessageLines.Add('regex:(?i)\bClaude Sonnet 5\b==>Claude')
    Set-Utf8NoBomContent $replaceMessageFile $replaceMessageLines

    $filterRepoArgs = New-Object System.Collections.Generic.List[string]
    $filterRepoArgs.Add('filter-repo'); $filterRepoArgs.Add('--force'); $filterRepoArgs.Add('--invert-paths')
    foreach ($path in $excludedPaths) {
        $filterRepoArgs.Add('--path'); $filterRepoArgs.Add($path)
    }
    $filterRepoArgs.Add('--mailmap'); $filterRepoArgs.Add($mailmapFile)
    $filterRepoArgs.Add('--replace-text'); $filterRepoArgs.Add($replaceTextFile)
    $filterRepoArgs.Add('--replace-message'); $filterRepoArgs.Add($replaceMessageFile)
    # filter-repo's default ('auto') prunes a commit that becomes empty after
    # --invert-paths removes its only changes (e.g. a commit that touched
    # only zamboni_local.db) -- silently contradicting this script's own
    # stated guarantee that "every commit on the current branch is kept."
    # 'never' on both forces every commit (and every merge, even one that
    # becomes degenerate -- fewer than two distinct parents -- after
    # filtering) to survive, matching that guarantee instead of the
    # commit-count check below just shrugging at a mismatch.
    $filterRepoArgs.Add('--prune-empty'); $filterRepoArgs.Add('never')
    $filterRepoArgs.Add('--prune-degenerate'); $filterRepoArgs.Add('never')

    git -C $isolatedRepo @filterRepoArgs
    Assert-LastExit "git filter-repo rewrite"

    Remove-Item $replaceTextFile -ErrorAction SilentlyContinue
    Remove-Item $replaceMessageFile -ErrorAction SilentlyContinue
}
else {
    $env:FILTER_BRANCH_SQUELCH_WARNING = "1"

    $envFilter = @"
if [ "`$GIT_AUTHOR_EMAIL" = "$oldEmail" ]; then
    export GIT_AUTHOR_NAME="$newName"
    export GIT_AUTHOR_EMAIL="$newEmail"
fi
if [ "`$GIT_COMMITTER_EMAIL" = "$oldEmail" ] || [ "`$GIT_COMMITTER_EMAIL" = "noreply@github.com" ]; then
    export GIT_COMMITTER_NAME="$newName"
    export GIT_COMMITTER_EMAIL="$newEmail"
fi
"@
    $pathList = ($excludedPaths -join ' ')
    $indexFilter = "git rm -r --cached --ignore-unmatch -- $pathList"

    git -C $isolatedRepo filter-branch -f --env-filter $envFilter --index-filter $indexFilter --tag-name-filter cat -- --all
    Assert-LastExit "git filter-branch rewrite"

    git -C $isolatedRepo for-each-ref --format='%(refname)' refs/original/ |
        ForEach-Object { git -C $isolatedRepo update-ref -d $_ }
    git -C $isolatedRepo reflog expire --expire=now --all
    git -C $isolatedRepo gc --prune=now *> $null

    # Content/message scrubbing across history isn't available without
    # filter-repo -- scrub the final tree only, as an explicit extra commit,
    # so at least HEAD is guaranteed clean.
    Write-Section "Scrubbing final tree (fallback mode -- HEAD only)"

    $tokenRules = New-Object System.Collections.Generic.List[object]
    foreach ($oldUser in $oldUsernames) {
        $tokenRules.Add([pscustomobject]@{ Old = "https://github.com/$oldUser/zamboni.git"; Token = '__ZAMBONI_ORG_REMOTE_GIT__' })
        $tokenRules.Add([pscustomobject]@{ Old = "git@github.com:$oldUser/zamboni.git";     Token = '__ZAMBONI_ORG_REMOTE_GIT__' })
        $tokenRules.Add([pscustomobject]@{ Old = "https://github.com/$oldUser/zamboni";     Token = '__ZAMBONI_ORG_REMOTE_DISPLAY__' })
        $tokenRules.Add([pscustomobject]@{ Old = "github.com/$oldUser/zamboni";             Token = '__ZAMBONI_ORG_REMOTE_DISPLAY__' })
    }
    $tokenRules.Add([pscustomobject]@{ Old = $oldEmail;             Token = '__ZAMBONI_ORG_EMAIL__' })
    $tokenRules.Add([pscustomobject]@{ Old = 'noreply@github.com';  Token = '__ZAMBONI_ORG_EMAIL__' })
    foreach ($oldUser in $oldUsernames) {
        $tokenRules.Add([pscustomobject]@{ Old = $oldUser; Token = '__ZAMBONI_ORG_USERNAME__' })
    }
    $tokenValues = [ordered]@{
        '__ZAMBONI_ORG_REMOTE_GIT__'     = $orgRemoteUrl
        '__ZAMBONI_ORG_REMOTE_DISPLAY__' = $publicRepoUrl
        '__ZAMBONI_ORG_EMAIL__'          = $newEmail
        '__ZAMBONI_ORG_USERNAME__'       = $usernameReplacement
    }

    $changedFiles = 0
    $exportFiles = Get-ChildItem -LiteralPath $isolatedRepo -Recurse -File -Force |
        Where-Object { -not $_.FullName.StartsWith((Join-Path $isolatedRepo '.git'), [System.StringComparison]::OrdinalIgnoreCase) }
    foreach ($file in $exportFiles) {
        $fileInfo = Read-Utf8TextFile $file.FullName
        if ($null -eq $fileInfo) { continue }

        $text = $fileInfo.Text
        $original = $text
        foreach ($rule in $tokenRules) {
            $result = Replace-LiteralIgnoreCase $text $rule.Old $rule.Token
            $text = $result.Text
        }
        foreach ($token in $tokenValues.Keys) {
            $text = $text.Replace($token, [string]$tokenValues[$token])
        }

        if ($text -cne $original) {
            $encoding = [System.Text.UTF8Encoding]::new([bool]$fileInfo.HasBom)
            [System.IO.File]::WriteAllText($file.FullName, $text, $encoding)
            $changedFiles += 1
        }
    }
    Write-Host "Scrubbed $changedFiles file(s) in the final tree."

    $treeViolationsSplit = Split-Violations (Find-ForbiddenReferences $isolatedRepo $forbiddenPatterns) $ambiguousPatterns
    if ($treeViolationsSplit.Ambiguous.Count -gt 0) {
        Write-Host "Expected matches in the final tree (your new identity legitimately contains an old pattern, not a leak):" -ForegroundColor Yellow
        $treeViolationsSplit.Ambiguous | ForEach-Object { Write-Host "  $_" }
    }
    if ($treeViolationsSplit.Fatal.Count -gt 0) {
        Write-Host "Forbidden references remain in the final tree after fallback scrub:" -ForegroundColor Red
        $treeViolationsSplit.Fatal | ForEach-Object { Write-Host "  $_" }
        exit 1
    }

    git -C $isolatedRepo add -A
    Assert-LastExit "stage fallback scrub changes"
    $fallbackStatus = @(git -C $isolatedRepo status --porcelain)
    if ($fallbackStatus.Count -gt 0) {
        git -C $isolatedRepo -c user.name="$newName" -c user.email="$newEmail" commit --quiet -m "chore: scrub public repo references from HEAD (fallback mode)"
        Assert-LastExit "commit fallback scrub"
    }
}

# ---------------------------------------------------------------------------
# 7. Verify across every commit -- not just HEAD
# ---------------------------------------------------------------------------
Write-Section "Post-rewrite verification (full history)"

$finalCommitCount = [int](git -C $isolatedRepo rev-list --all --count)
Assert-LastExit "count rewritten commits"
if ($finalCommitCount -eq 0) {
    Write-Host "Rewrite produced zero commits. Refusing to push." -ForegroundColor Red
    exit 1
}
Write-Host "Commits before rewrite: $sourceCommitCount"
Write-Host "Commits after rewrite : $finalCommitCount"
# --prune-empty never --prune-degenerate never (filter-repo path) and the
# fallback's filter-branch invocation (which never passes --prune-empty at
# all) both guarantee no commit is ever dropped -- so unlike before, a
# mismatch here now means something unexpected happened, not an accepted
# side effect of pruning. Refusing to push is safer than silently trusting
# a rewrite that didn't preserve the commit this script promises to.
if ($finalCommitCount -ne $sourceCommitCount) {
    Write-Host "Commit count changed during rewrite ($sourceCommitCount -> $finalCommitCount). This script guarantees every commit survives (no pruning is requested) -- refusing to push." -ForegroundColor Red
    exit 1
}

$identityLines = @(git -C $isolatedRepo log --all --format='%an|%ae|%cn|%ce' | Select-Object -Unique)
Assert-LastExit "read commit identities across history"
$expectedIdentityLine = "$newName|$newEmail|$newName|$newEmail"
if ($identityLines.Count -ne 1 -or $identityLines[0] -cne $expectedIdentityLine) {
    Write-Host "Identity verification failed -- not every commit maps to the new identity:" -ForegroundColor Red
    $identityLines | ForEach-Object { Write-Host "  $_" }
    exit 1
}
Write-Host "Every commit's author/committer is $newName <$newEmail>." -ForegroundColor Green

$contentViolations = @(Test-HistoryClean $isolatedRepo $forbiddenPatterns)
$pathViolations = @(Get-HistoricalPathViolations $isolatedRepo $forbiddenPatterns)
$messageViolations = @(Test-CommitMessagesClean $isolatedRepo $forbiddenPatterns)
$allViolationsSplit = Split-Violations (@($contentViolations) + @($pathViolations) + @($messageViolations)) $ambiguousPatterns
$allViolations = $allViolationsSplit.Fatal

if ($allViolationsSplit.Ambiguous.Count -gt 0) {
    Write-Host "Expected matches across history (your new identity legitimately contains an old pattern, not a leak):" -ForegroundColor Yellow
    $allViolationsSplit.Ambiguous | ForEach-Object { Write-Host "  $_" }
}

if ($allViolations.Count -gt 0) {
    if ($haveFilterRepo) {
        Write-Host "Forbidden references remain across history after a full-history rewrite -- this is unexpected:" -ForegroundColor Red
        $allViolations | ForEach-Object { Write-Host "  $_" }
        exit 1
    }
    else {
        Write-Host "Forbidden references remain in OLDER commits (expected in fallback mode -- HEAD itself is clean):" -ForegroundColor Yellow
        $allViolations | ForEach-Object { Write-Host "  $_" }
        Write-Host "Review the list above before pushing. Re-run with git-filter-repo installed for full coverage." -ForegroundColor Yellow
    }
}
else {
    Write-Host "No unexpected forbidden references found anywhere in history." -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# 8. Point at the org remote and push
# ---------------------------------------------------------------------------
Write-Section "Adding org remote"

git -C $isolatedRepo remote add $remoteName $orgRemoteUrl
Assert-LastExit "add org remote to isolated repository"
Write-Host "Remote '$remoteName' -> $orgRemoteUrl"
Write-Host "Prepared branch tip:" -ForegroundColor Green
git -C $isolatedRepo log -1 --format='  %h  %an <%ae>  %s'

$pushRef = 'HEAD'  # what actually gets pushed -- becomes a rebuilt branch below if $orgBaseBranch is set

if ($orgBaseBranch) {
    Write-Section "Fetching org branch '$orgBaseBranch'"
    $baseFetchResult = Invoke-GitCapture -Arguments @('-C', $isolatedRepo, 'fetch', $remoteName, $orgBaseBranch)
    if ($baseFetchResult.ExitCode -ne 0) {
        Write-Host "Could not fetch org branch '$orgBaseBranch' -- it may not exist yet. Falling back to pushing a standalone new branch." -ForegroundColor Yellow
        $baseFetchResult.Output | ForEach-Object { Write-Host "  $_" }
        $orgBaseBranch = ''
    }
}

if ($orgBaseBranch) {
    Write-Section "Rebuilding onto '$orgBaseBranch' (so the PR shows a real diff)"

    # git format-patch/git am replay each commit as an independent diff --
    # correct for a linear history (this script's design goal, "every commit
    # kept"), but a merge commit's second parent can't be reconstructed this
    # way. Confirmed there's no silent partial-replay risk by refusing
    # outright if any merge commit exists, rather than only warning.
    $mergeCommits = @(git -C $isolatedRepo rev-list --min-parents=2 --all)
    Assert-LastExit "check for merge commits"
    if ($mergeCommits.Count -gt 0) {
        Write-Host "The rewritten history contains $($mergeCommits.Count) merge commit(s). git format-patch/git am cannot losslessly replay a merge commit onto another branch -- rebuilding onto '$orgBaseBranch' is not safe here." -ForegroundColor Red
        Write-Host "Re-run and leave the base-branch prompt blank to push as a standalone new branch instead, or handle this merge history manually." -ForegroundColor Red
        exit 1
    }

    $patchDir = Join-Path $migrationRoot 'patches'
    New-Item -ItemType Directory -Path $patchDir *> $null
    git -C $isolatedRepo format-patch --root --output-directory $patchDir HEAD *> $null
    Assert-LastExit "generate patches from rewritten history"
    $patchFiles = @(Get-ChildItem -LiteralPath $patchDir -Filter '*.patch' | Sort-Object Name | ForEach-Object { $_.FullName })
    if ($patchFiles.Count -ne $finalCommitCount) {
        Write-Host "Generated $($patchFiles.Count) patch(es) but expected $finalCommitCount -- refusing to proceed." -ForegroundColor Red
        exit 1
    }
    Write-Host "Generated $($patchFiles.Count) patch(es)."

    $rebuildBranch = "$targetBranch-onto-$orgBaseBranch"
    git -C $isolatedRepo checkout -b $rebuildBranch "$remoteName/$orgBaseBranch"
    Assert-LastExit "create rebuild branch from org '$orgBaseBranch'"

    $amResult = Invoke-GitCapture -Arguments (@('-C', $isolatedRepo, '-c', "user.name=$newName", '-c', "user.email=$newEmail", 'am') + $patchFiles)
    if ($amResult.ExitCode -ne 0) {
        Write-Host "Applying patches onto '$orgBaseBranch' failed -- a real conflict between these changes and what's already on that branch:" -ForegroundColor Red
        $amResult.Output | ForEach-Object { Write-Host "  $_" }
        Write-Host "Resolve manually inside $isolatedRepo (git am --show-current-patch to see the stuck patch, git am --abort to bail out and try something else), or re-run with a different/blank base branch." -ForegroundColor Red
        exit 1
    }
    Write-Host "Applied $($patchFiles.Count) patch(es) onto '$orgBaseBranch'." -ForegroundColor Green

    $rebuiltCount = [int](git -C $isolatedRepo rev-list "$remoteName/$orgBaseBranch..HEAD" --count)
    Assert-LastExit "count rebuilt commits"
    if ($rebuiltCount -ne $finalCommitCount) {
        Write-Host "Rebuilt branch has $rebuiltCount commit(s) ahead of '$orgBaseBranch', expected $finalCommitCount -- refusing to push." -ForegroundColor Red
        exit 1
    }
    $pushRef = $rebuildBranch
    Write-Host "This branch now shares real history with '$orgBaseBranch' -- a PR against it will show a normal diff instead of 'unrelated histories.'" -ForegroundColor Green
}

Write-Section "Validating push"

git -C $isolatedRepo push --dry-run $remoteName "${pushRef}:refs/heads/$targetBranch"
Assert-LastExit "dry-run push"
Write-Host "Dry-run push succeeded." -ForegroundColor Green
Write-Host "About to create NEW remote branch '$targetBranch' with $finalCommitCount commit(s)$(if ($orgBaseBranch) { " on top of '$orgBaseBranch'" })."
if ($allViolations.Count -gt 0) {
    Write-Host "NOTE: known residual references in older commits are listed above." -ForegroundColor Yellow
}
Confirm-Word "Push the rewritten history now" "PUSH"

git -C $isolatedRepo push -u $remoteName "${pushRef}:refs/heads/$targetBranch"
Assert-LastExit "push rewritten history"

Write-Section "Migration complete"
git -C $isolatedRepo log -1 --format='  %H  %an <%ae>  %s' $pushRef
Write-Host ""
Write-Host "Isolated rewritten repository: $isolatedRepo"
Write-Host "Sensitive source-history backup: $script:backupPath" -ForegroundColor Yellow
Write-Host "Verify the org repository independently before deleting either local artifact."
Write-Host "The source repository was not modified. Tags/other branches were not pushed."
if ($orgBaseBranch) {
    Write-Host "Open a PR from '$targetBranch' into '$orgBaseBranch' on the org remote -- it now shares real history, so the diff will show your actual changes." -ForegroundColor Green
}
