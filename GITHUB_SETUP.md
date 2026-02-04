# GitHub Repository Setup Instructions

## Step 1: Create a GitHub Repository
1. Go to [GitHub](https://github.com) and log in
2. Click the "+" icon in the top right and select "New repository"
3. Choose a repository name (e.g., "discord-bot")
4. Choose whether it should be Public or Private
5. **DO NOT** initialize with README, .gitignore, or license (we already have these)
6. Click "Create repository"

## Step 2: Connect Your Local Repository to GitHub
After creating the repository, GitHub will show you commands. You'll use the commands for "push an existing repository from the command line":

```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git branch -M main
git push -u origin main
```

**Replace** `YOUR_USERNAME` and `YOUR_REPO_NAME` with your actual GitHub username and repository name.

## Step 3: Configure Your Git User (if not already done)
If you need to update your email (currently set to example.com):
```bash
git config --global user.email "your-actual-email@example.com"
git config --global user.name "Your Name"
```

## Step 4: Future Updates
After making changes to your code:

```bash
# Check what has changed
git status

# Add all changes
git add .

# Or add specific files
git add filename.py

# Commit with a descriptive message
git commit -m "Description of what you changed"

# Push to GitHub
git push
```

## Important Files That Are Ignored
The following files are excluded from Git (see .gitignore):
- API keys and tokens (API_KEY.txt, BOT_TOKEN.txt)
- Database files (*.db)
- User data and snapshots
- Python cache (__pycache__)
- Virtual environment (.venv/)
- Backup directories
- Migration and utility scripts

## Creating a README Template
Consider adding information to your README.md about:
- What the bot does
- How to set it up
- Required environment variables
- Dependencies (link to requirements.txt)
- How to run the bot

## Security Reminder
⚠️ **NEVER** commit your API keys, bot tokens, or database files to GitHub!
The .gitignore file is configured to prevent this, but always double-check with `git status` before committing.
