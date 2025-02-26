"""
Git operations
"""
import os
import asyncio
from typing import Optional, List

from app.core.config import settings
from app.core.logging import logger


class GitManagerService:
    """Service for Git operations"""
    
    async def init_repo(self, repo_path: str) -> None:
        """
        Initialize a Git repository
        """
        # Check if repo is already initialized
        if os.path.exists(os.path.join(repo_path, ".git")):
            logger.info(f"Git repository already initialized at {repo_path}")
            return
            
        try:
            # Initialize Git repo
            process = await asyncio.create_subprocess_exec(
                "git", "init",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to initialize Git repository: {error_msg}")
                raise Exception(f"Failed to initialize Git repository: {error_msg}")
                
            logger.info(f"Initialized Git repository at {repo_path}")
            
            # Configure remote if URL is provided
            if settings.GIT_REPO_URL:
                await self.configure_remote(settings.GIT_REPO_URL, repo_path)
                
        except Exception as e:
            logger.error(f"Error initializing Git repository: {str(e)}")
            raise
    
    async def configure_remote(self, remote_url: str, repo_path: str) -> None:
        """
        Configure remote repository
        """
        try:
            # Check if remote already exists
            process = await asyncio.create_subprocess_exec(
                "git", "remote",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if "origin" in stdout.decode():
                # Update existing remote
                process = await asyncio.create_subprocess_exec(
                    "git", "remote", "set-url", "origin", remote_url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=repo_path
                )
            else:
                # Add new remote
                process = await asyncio.create_subprocess_exec(
                    "git", "remote", "add", "origin", remote_url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=repo_path
                )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to configure remote: {error_msg}")
                raise Exception(f"Failed to configure remote: {error_msg}")
                
            logger.info(f"Configured remote 'origin' with URL {remote_url}")
            
        except Exception as e:
            logger.error(f"Error configuring remote: {str(e)}")
            raise
    
    async def create_branch(self, branch_name: str, repo_path: str) -> None:
        """
        Create and checkout a new branch
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "git", "checkout", "-b", branch_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to create branch: {error_msg}")
                raise Exception(f"Failed to create branch: {error_msg}")
                
            logger.info(f"Created and checked out branch '{branch_name}'")
            
        except Exception as e:
            logger.error(f"Error creating branch: {str(e)}")
            raise
    
    async def commit_changes(self, commit_message: str, repo_path: str) -> None:
        """
        Add and commit all changes
        """
        try:
            # Add all changes
            process = await asyncio.create_subprocess_exec(
                "git", "add", ".",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to add files: {error_msg}")
                raise Exception(f"Failed to add files: {error_msg}")
            
            # Commit changes
            process = await asyncio.create_subprocess_exec(
                "git", "commit", "-m", commit_message,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            # If there are no changes to commit, this is not an error
            if process.returncode != 0 and "nothing to commit" not in stderr.decode():
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to commit changes: {error_msg}")
                raise Exception(f"Failed to commit changes: {error_msg}")
                
            logger.info(f"Committed changes with message: {commit_message}")
            
        except Exception as e:
            logger.error(f"Error committing changes: {str(e)}")
            raise
    
    async def push_changes(self, branch_name: str, repo_path: str) -> None:
        """
        Push changes to remote repository
        """
        try:
            # Set Git credentials if provided
            if settings.GIT_USERNAME and settings.GIT_PASSWORD:
                repo_url_with_auth = settings.GIT_REPO_URL.replace(
                    "https://", 
                    f"https://{settings.GIT_USERNAME}:{settings.GIT_PASSWORD}@"
                )
                
                process = await asyncio.create_subprocess_exec(
                    "git", "remote", "set-url", "origin", repo_url_with_auth,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=repo_path
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    logger.error(f"Failed to set remote URL with credentials: {error_msg}")
                    raise Exception(f"Failed to set remote URL with credentials: {error_msg}")
            
            # Push to remote
            process = await asyncio.create_subprocess_exec(
                "git", "push", "-u", "origin", branch_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to push changes: {error_msg}")
                raise Exception(f"Failed to push changes: {error_msg}")
                
            logger.info(f"Pushed changes to remote branch '{branch_name}'")
            
            # Reset remote URL to remove credentials
            if settings.GIT_USERNAME and settings.GIT_PASSWORD:
                process = await asyncio.create_subprocess_exec(
                    "git", "remote", "set-url", "origin", settings.GIT_REPO_URL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=repo_path
                )
                
                stdout, stderr = await process.communicate()
                
        except Exception as e:
            logger.error(f"Error pushing changes: {str(e)}")
            raise
    
    async def clone_repo(self, repo_url: str, target_path: str) -> None:
        """
        Clone a repository
        """
        try:
            # Set Git credentials if provided
            clone_url = repo_url
            if settings.GIT_USERNAME and settings.GIT_PASSWORD:
                clone_url = repo_url.replace(
                    "https://", 
                    f"https://{settings.GIT_USERNAME}:{settings.GIT_PASSWORD}@"
                )
            
            process = await asyncio.create_subprocess_exec(
                "git", "clone", clone_url, target_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to clone repository: {error_msg}")
                raise Exception(f"Failed to clone repository: {error_msg}")
                
            logger.info(f"Cloned repository {repo_url} to {target_path}")
            
        except Exception as e:
            logger.error(f"Error cloning repository: {str(e)}")
            raise
    
    async def get_current_branch(self, repo_path: str) -> str:
        """
        Get the name of the current branch
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "git", "rev-parse", "--abbrev-ref", "HEAD",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to get current branch: {error_msg}")
                raise Exception(f"Failed to get current branch: {error_msg}")
                
            branch_name = stdout.decode().strip()
            logger.info(f"Current branch: {branch_name}")
            
            return branch_name
            
        except Exception as e:
            logger.error(f"Error getting current branch: {str(e)}")
            raise
    
    async def list_branches(self, repo_path: str) -> List[str]:
        """
        List all branches in the repository
        """
        try:
            process = await asyncio.create_subprocess_exec(
                "git", "branch", "--list",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=repo_path
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Failed to list branches: {error_msg}")
                raise Exception(f"Failed to list branches: {error_msg}")
                
            # Parse branch names (remove asterisk and whitespace)
            branches = []
            for line in stdout.decode().strip().split("\n"):
                branch = line.strip()
                if branch.startswith("*"):
                    branch = branch[1:].strip()
                branches.append(branch)
                
            return branches
            
        except Exception as e:
            logger.error(f"Error listing branches: {str(e)}")
            raise
