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
    - Every user-supplied replacement value is itself checked against the
      forbidden-pattern list, so a copy-paste mistake can't reintroduce the
      old identity through the "new" value.
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
foreach ($entry in $deliverableInputs.GetEnumerator()) {
    foreach ($pattern in $forbiddenPatterns) {
        if ([string]$entry.Value -and
            ([string]$entry.Value).IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            Write-Host "The $($entry.Key) still contains forbidden legacy value '$pattern'." -ForegroundColor Red
            exit 1
        }
    }
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
    # Regex mode with an inline case-insensitive flag, most-specific pattern
    # first -- e.g. the full ".git"-suffixed clone URL must be consumed before
    # the bare-username rule gets a chance to partially mangle it, since
    # filter-repo applies each line in file order to the running text. Built
    # per old username so multiple historical usernames are all covered.
    $replaceTextFile = Join-Path $migrationRoot 'replace-text.txt'
    $replaceTextLines = New-Object System.Collections.Generic.List[string]
    foreach ($oldUser in $oldUsernames) {
        $replaceTextLines.Add("regex:(?i)https://github\.com/$oldUser/zamboni\.git==>$orgRemoteUrl")
        $replaceTextLines.Add("regex:(?i)git@github\.com:$oldUser/zamboni\.git==>$orgRemoteUrl")
        $replaceTextLines.Add("regex:(?i)https://github\.com/$oldUser/zamboni==>$publicRepoUrl")
        $replaceTextLines.Add("regex:(?i)github\.com/$oldUser/zamboni==>$publicRepoUrl")
    }
    $escapedOldEmail = [System.Text.RegularExpressions.Regex]::Escape($oldEmail)
    $replaceTextLines.Add("regex:(?i)$escapedOldEmail==>$newEmail")
    $replaceTextLines.Add("regex:(?i)noreply@github\.com==>$newEmail")
    foreach ($oldUser in $oldUsernames) {
        $replaceTextLines.Add("regex:(?i)$oldUser==>$usernameReplacement")
    }
    Set-Utf8NoBomContent $replaceTextFile $replaceTextLines

    $filterRepoArgs = New-Object System.Collections.Generic.List[string]
    $filterRepoArgs.Add('filter-repo'); $filterRepoArgs.Add('--force'); $filterRepoArgs.Add('--invert-paths')
    foreach ($path in $excludedPaths) {
        $filterRepoArgs.Add('--path'); $filterRepoArgs.Add($path)
    }
    $filterRepoArgs.Add('--mailmap'); $filterRepoArgs.Add($mailmapFile)
    $filterRepoArgs.Add('--replace-text'); $filterRepoArgs.Add($replaceTextFile)
    $filterRepoArgs.Add('--replace-message'); $filterRepoArgs.Add($replaceTextFile)

    git -C $isolatedRepo @filterRepoArgs
    Assert-LastExit "git filter-repo rewrite"

    Remove-Item $replaceTextFile -ErrorAction SilentlyContinue
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

    $treeViolations = @(Find-ForbiddenReferences $isolatedRepo $forbiddenPatterns)
    if ($treeViolations.Count -gt 0) {
        Write-Host "Forbidden references remain in the final tree after fallback scrub:" -ForegroundColor Red
        $treeViolations | ForEach-Object { Write-Host "  $_" }
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
if ($finalCommitCount -ne $sourceCommitCount) {
    Write-Host "(Difference is expected if any commit became empty after removing excluded paths -- filter-repo/filter-branch prune those.)" -ForegroundColor Yellow
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
$allViolations = @($contentViolations) + @($pathViolations)

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
    Write-Host "No forbidden references found anywhere in history." -ForegroundColor Green
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

Write-Section "Validating push"

git -C $isolatedRepo push --dry-run $remoteName "HEAD:refs/heads/$targetBranch"
Assert-LastExit "dry-run push"
Write-Host "Dry-run push succeeded." -ForegroundColor Green
Write-Host "About to create NEW remote branch '$targetBranch' with $finalCommitCount commit(s)."
if ($allViolations.Count -gt 0) {
    Write-Host "NOTE: known residual references in older commits are listed above." -ForegroundColor Yellow
}
Confirm-Word "Push the rewritten history now" "PUSH"

git -C $isolatedRepo push -u $remoteName "HEAD:refs/heads/$targetBranch"
Assert-LastExit "push rewritten history"

Write-Section "Migration complete"
git -C $isolatedRepo log -1 --format='  %H  %an <%ae>  %s'
Write-Host ""
Write-Host "Isolated rewritten repository: $isolatedRepo"
Write-Host "Sensitive source-history backup: $script:backupPath" -ForegroundColor Yellow
Write-Host "Verify the org repository independently before deleting either local artifact."
Write-Host "The source repository was not modified. Tags/other branches were not pushed."
