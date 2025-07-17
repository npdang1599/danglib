# Project-Specific Instructions for danglib

## Git Workflow Reminders

### ALWAYS Create a Branch Before Committing
- **Never commit directly to master**
- Before staging any files, create an appropriate branch following the conventions in docs/git_conventions.md:
  - `feature/description` for new features
  - `fix/description` for bug fixes
  - `docs/description` for documentation updates
  - `refactor/description` for code refactoring
  - `chore/description` for maintenance tasks

### Example Workflow
```bash
# 1. First check status
git status

# 2. Create branch BEFORE adding files
git checkout -b <branch-type>/<description>

# 3. Then stage and commit
git add <files>
git commit -m "<type>: <brief description>"
```

## Additional Project Notes
- This project uses the git conventions documented in docs/git_conventions.md
- Always follow the branch naming and commit message formats specified there