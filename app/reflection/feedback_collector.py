"""
Feedback collector for collecting user feedback.
"""
from typing import Any, Dict, List, Optional, Union, Tuple
import time
import json
import os
from pathlib import Path

from ..config.logging_config import logger
from ..config.settings import settings, BASE_DIR


class FeedbackCollector:
    """
    Collector for user feedback on queries.
    """
    
    def __init__(self):
        """Initialize the feedback collector."""
        self.feedback_dir = BASE_DIR / "feedback"
        self.feedback_dir.mkdir(exist_ok=True)
        
    async def collect_feedback(
        self, 
        query_id: str, 
        feedback: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Collect user feedback on a query.
        
        Args:
            query_id: Identifier for the query.
            feedback: User feedback.
            
        Returns:
            Dict[str, Any]: Result of feedback collection.
        """
        try:
            # Validate feedback
            is_valid, reason = self._validate_feedback(feedback)
            if not is_valid:
                return {
                    "success": False,
                    "error": f"Invalid feedback: {reason}"
                }
                
            # Add metadata
            feedback_with_metadata = {
                "query_id": query_id,
                "timestamp": time.time(),
                "feedback": feedback
            }
            
            # Store feedback
            filename = self.feedback_dir / f"{query_id}_{int(time.time())}.json"
            
            with open(filename, 'w') as f:
                json.dump(feedback_with_metadata, f, indent=2)
                
            logger.info(f"Collected feedback for query {query_id}: {feedback.get('rating', 'unknown')}")
            
            # Process feedback for learning
            await self._process_feedback(query_id, feedback)
            
            return {
                "success": True,
                "message": "Feedback collected successfully"
            }
            
        except Exception as e:
            logger.error(f"Error collecting feedback: {str(e)}")
            return {
                "success": False,
                "error": f"Error collecting feedback: {str(e)}"
            }

    def get_feedback_history(
        self, 
        query_id: Optional[str] = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get feedback history.
        
        Args:
            query_id: Optional query ID to filter by.
            limit: Maximum number of feedback items to return.
            
        Returns:
            Dict[str, Any]: Feedback history.
        """
        try:
            feedback_files = list(self.feedback_dir.glob("*.json"))
            feedback_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            feedback_items = []
            count = 0
            
            for file in feedback_files:
                if count >= limit:
                    break
                    
                try:
                    with open(file, 'r') as f:
                        feedback_data = json.load(f)
                        
                    # Filter by query_id if provided
                    if query_id and feedback_data.get("query_id") != query_id:
                        continue
                        
                    feedback_items.append(feedback_data)
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Error reading feedback file {file}: {str(e)}")
            
            return {
                "success": True,
                "feedback_items": feedback_items,
                "count": len(feedback_items)
            }
            
        except Exception as e:
            logger.error(f"Error getting feedback history: {str(e)}")
            return {
                "success": False,
                "error": f"Error getting feedback history: {str(e)}"
            }

    def get_feedback_stats(self) -> Dict[str, Any]:
        """
        Get statistics on collected feedback.
        
        Returns:
            Dict[str, Any]: Feedback statistics.
        """
        try:
            feedback_files = list(self.feedback_dir.glob("*.json"))
            
            if not feedback_files:
                return {
                    "success": True,
                    "message": "No feedback collected yet",
                    "stats": {}
                }
                
            # Process all feedback files
            ratings = []
            accuracies = []
            relevancies = []
            performance_ratings = []
            
            for file in feedback_files:
                try:
                    with open(file, 'r') as f:
                        feedback_data = json.load(f)
                        
                    feedback = feedback_data.get("feedback", {})
                    
                    if "rating" in feedback:
                        ratings.append(feedback["rating"])
                        
                    if "accuracy" in feedback:
                        accuracies.append(feedback["accuracy"])
                        
                    if "relevance" in feedback:
                        relevancies.append(feedback["relevance"])
                        
                    if "performance" in feedback:
                        performance_ratings.append(feedback["performance"])
                        
                except Exception as e:
                    logger.error(f"Error reading feedback file {file}: {str(e)}")
            
            # Calculate statistics
            stats = {
                "total_feedback": len(feedback_files),
                "average_rating": self._calculate_average(ratings),
                "average_accuracy": self._calculate_average(accuracies),
                "average_relevance": self._calculate_average(relevancies),
                "average_performance": self._calculate_average(performance_ratings),
                "rating_distribution": self._calculate_distribution(ratings),
                "common_issues": self._identify_common_issues(feedback_files)
            }
            
            return {
                "success": True,
                "stats": stats
            }
            
        except Exception as e:
            logger.error(f"Error getting feedback stats: {str(e)}")
            return {
                "success": False,
                "error": f"Error getting feedback stats: {str(e)}"
            }

    def _validate_feedback(self, feedback: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate user feedback.
        
        Args:
            feedback: User feedback to validate.
            
        Returns:
            Tuple[bool, str]: (True, "") if valid, (False, reason) if invalid.
        """
        # Check if feedback is empty
        if not feedback:
            return False, "Feedback is empty"
            
        # Check if rating is present and valid
        if "rating" in feedback:
            rating = feedback["rating"]
            if not isinstance(rating, (int, float)) or rating < 1 or rating > 5:
                return False, "Rating must be a number between 1 and 5"
                
        # Check if comment is valid
        if "comment" in feedback and not isinstance(feedback["comment"], str):
            return False, "Comment must be a string"
            
        # Check if accuracy is valid
        if "accuracy" in feedback:
            accuracy = feedback["accuracy"]
            if not isinstance(accuracy, (int, float)) or accuracy < 1 or accuracy > 5:
                return False, "Accuracy must be a number between 1 and 5"
                
        # Check if relevance is valid
        if "relevance" in feedback:
            relevance = feedback["relevance"]
            if not isinstance(relevance, (int, float)) or relevance < 1 or relevance > 5:
                return False, "Relevance must be a number between 1 and 5"
                
        # Check if performance is valid
        if "performance" in feedback:
            performance = feedback["performance"]
            if not isinstance(performance, (int, float)) or performance < 1 or performance > 5:
                return False, "Performance must be a number between 1 and 5"
                
        return True, ""

    async def _process_feedback(
        self, 
        query_id: str, 
        feedback: Dict[str, Any]
    ) -> None:
        """
        Process feedback for learning.
        
        Args:
            query_id: Identifier for the query.
            feedback: User feedback.
        """
        # This would typically involve some machine learning or analytics
        # For now, we'll just log the feedback
        rating = feedback.get("rating", "unknown")
        comment = feedback.get("comment", "")
        
        logger.info(f"Processing feedback for query {query_id}: Rating={rating}, Comment={comment}")
        
        # In a real application, this would update a feedback database or ML model

    def _calculate_average(self, values: List[Union[int, float]]) -> Optional[float]:
        """
        Calculate the average of values.
        
        Args:
            values: List of values.
            
        Returns:
            Optional[float]: Average, or None if no values.
        """
        if not values:
            return None
            
        return sum(values) / len(values)

    def _calculate_distribution(
        self, 
        values: List[Union[int, float]]
    ) -> Dict[Union[int, float], int]:
        """
        Calculate the distribution of values.
        
        Args:
            values: List of values.
            
        Returns:
            Dict[Union[int, float], int]: Distribution of values.
        """
        distribution = {}
        
        for value in values:
            if value not in distribution:
                distribution[value] = 0
                
            distribution[value] += 1
            
        return distribution

    def _identify_common_issues(self, feedback_files: List[Path]) -> List[str]:
        """
        Identify common issues from feedback.
        
        Args:
            feedback_files: List of feedback files.
            
        Returns:
            List[str]: Common issues.
        """
        issues = {}
        
        for file in feedback_files:
            try:
                with open(file, 'r') as f:
                    feedback_data = json.load(f)
                    
                feedback = feedback_data.get("feedback", {})
                
                if "issues" in feedback:
                    for issue in feedback["issues"]:
                        if issue not in issues:
                            issues[issue] = 0
                            
                        issues[issue] += 1
                        
                # Check for low ratings with comments
                rating = feedback.get("rating", 5)
                comment = feedback.get("comment", "")
                
                if rating <= 2 and comment:
                    # This is a simplified approach
                    # In a real application, this would use NLP for issue extraction
                    issue = f"Low rating: {comment[:50]}..."
                    
                    if issue not in issues:
                        issues[issue] = 0
                        
                    issues[issue] += 1
                    
            except Exception as e:
                logger.error(f"Error processing feedback file {file}: {str(e)}")
        
        # Sort issues by frequency
        sorted_issues = sorted(issues.items(), key=lambda x: x[1], reverse=True)
        
        # Return top 5 issues
        return [issue for issue, count in sorted_issues[:5]]


# Create global feedback collector instance
feedback_collector = FeedbackCollector()