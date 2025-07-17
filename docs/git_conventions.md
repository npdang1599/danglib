# Git Conventions and Usage Guide

## Overview

This document outlines the Git conventions and best practices for the danglib project. All team members should follow these guidelines to maintain a clean and consistent Git history.

## Branch Naming Conventions

### Format Rules
- Use lowercase letters only
- Separate words with dashes (-)
- No spaces, underscores, or special characters
- Keep names concise but descriptive
- This format ensures branches display cleanly in URLs (GitHub, GitLab, etc.)

### Branch Types
- **Feature branches**: `feature/descriptive-name` - New functionality
- **Bug fix branches**: `fix/issue-description` - Fixing broken behavior
- **Documentation branches**: `docs/what-is-being-documented` - Documentation updates
- **Refactoring branches**: `refactor/what-is-being-refactored` - Code improvements without changing functionality
- **Hotfix branches**: `hotfix/critical-issue` - Urgent production fixes
- **Chore branches**: `chore/task-description` - Maintenance tasks that don't modify source code or tests
  - Examples: updating dependencies, modifying build scripts, cleaning up unused files, updating configuration

### Examples
- ✅ `feature/add-rsi-indicator`
- ✅ `fix/memory-leak-in-data-loader`
- ✅ `chore/update-numpy-version`
- ✅ `chore/clean-unused-imports`
- ❌ `feature/Add_RSI_Indicator` (wrong: uppercase and underscores)
- ❌ `fix/bug #123` (wrong: special characters)

## Commit Message Format

### Structure
Keep commits short and suitable for `git commit -m`:
```
<type>: <brief description>
```

### Types
- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting, missing semicolons, etc.)
- **refactor**: Code refactoring without changing functionality
- **test**: Adding or updating tests
- **chore**: Maintenance tasks, dependency updates

### Examples
- ✅ `feat: add RSI indicator calculation`
- ✅ `fix: resolve memory leak in data loader`
- ✅ `chore: update numpy to 1.24.0`
- ✅ `docs: update API endpoint examples`
- ❌ `Added new feature` (missing type prefix)
- ❌ `feat: implemented new RSI indicator with vectorized calculation and boundary checks` (too long)

### Guidelines
- Keep under 50 characters
- Use present tense ("add" not "added")
- No period at the end
- Save detailed explanations for PR description

## Pull Request Guidelines

1. **Title Format**: Use the same format as commit messages
2. **Description Template**: Use the template in `pull_request_template.md`
3. **Review Requirements**: At least one approval before merging
4. **Testing**: All tests must pass before merging

## Workflow Best Practices

### Before Starting Work
1. Ensure your local master is up to date:
   ```bash
   git checkout master
   git pull origin master
   ```

2. Create a new branch from master:
   ```bash
   git checkout -b feature/your-feature-name
   ```

### During Development
1. **Commit Frequently**: Make small, logical commits
2. **Atomic Commits**: Each commit should represent one logical change
3. **Review at Hunk Level**: Stage specific changes, not entire files
   - Command line: `git add -p` for interactive staging
   - VS Code: Use Source Control panel to stage individual changes
   - Other GUIs: GitKraken, SourceTree, GitHub Desktop support partial staging
4. **Test Before Committing**: Ensure code works before committing

### Before Creating PR
1. Update your branch with latest master:
   ```bash
   git checkout master
   git pull origin master
   git checkout your-branch
   git rebase master
   ```

2. Clean up commit history if needed:
   ```bash
   git rebase -i master
   ```

## Code Review Process

1. **Self-Review First**: Review your own changes before requesting review
2. **Check for Typos**: Run spell check on modified files
3. **Verify No Debug Code**: Remove console.logs, print statements
4. **Update Documentation**: If behavior changes, update relevant docs

## Special Considerations for Live Deployments

When making changes to production servers:

1. **Dependency Order**: Make changes in dependency order
   - Create new functions/modules first
   - Update references after dependencies are in place
   
2. **Versioning Strategy**: Implement feature flags or version switches for:
   - Major algorithm changes
   - API endpoint modifications
   - Data structure updates

3. **Rollback Plan**: Always ensure changes can be reverted:
   - Keep old code commented with version tags
   - Document rollback procedures
   - Test rollback process in staging

## Handling Large Datasets

Since we work with large financial datasets:

1. **Avoid Large File Commits**: Use `.gitignore` for data files
2. **Document Data Dependencies**: Note required data files in README
3. **Use Git LFS**: For necessary large files (models, etc.)

## Security Guidelines

1. **Never Commit Secrets**: API keys, passwords, tokens
2. **Use Environment Variables**: For configuration values
3. **Review for Sensitive Data**: Before committing, check for:
   - Internal server names
   - Database connection strings
   - Customer data

## Reverting Changes

If you need to revert changes:

1. **For Single Commit**: `git revert <commit-hash>`
2. **For Multiple Commits**: `git revert <oldest-commit>..<newest-commit>`
3. **Document Reason**: In revert commit message, explain why

## Tips for Team Collaboration

1. **Communicate Intent**: Use descriptive branch and commit names
2. **Link to Issues**: Reference issue numbers in commits
3. **Update Team**: Notify team of breaking changes
4. **Document Decisions**: Add comments for complex logic

## Common Commands Reference

```bash
# Stage specific hunks
git add -p

# View changes before committing
git diff --staged

# Amend last commit (before push)
git commit --amend

# Interactive rebase
git rebase -i master

# View commit history with graph
git log --oneline --graph --all

# Stash changes temporarily
git stash save "work in progress"

# Apply stashed changes
git stash pop
```

## Questions or Updates

If you have questions about these conventions or suggestions for improvements, please discuss with the team lead or create a PR to update this document.