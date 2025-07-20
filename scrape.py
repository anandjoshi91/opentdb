#!/usr/bin/env python3
"""
Open Trivia Database Scraper

Scrapes quiz questions from opentdb.com and saves them to CSV format.
Features:
- Handles retries and rate limiting
- Avoids duplicates using session tokens
- Fetches all available categories
- Decodes HTML entities
- Combines correct and incorrect answers into options
"""

import requests
import csv
import time
import html
import hashlib
import random
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Question:
    """Data class to represent a trivia question"""
    category: str
    question: str
    options: List[str]
    correct_answer: str
    difficulty: str
    question_type: str
    question_hash: str

class OpenTriviaDBScraper:
    """Scraper for Open Trivia Database API"""
    
    BASE_URL = "https://opentdb.com/api.php"
    CATEGORY_URL = "https://opentdb.com/api_category.php"
    TOKEN_URL = "https://opentdb.com/api_token.php"
    
    def __init__(self, output_file: str = "trivia_questions.csv", max_retries: int = 3):
        self.output_file = output_file
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session_token: Optional[str] = None
        self.seen_hashes = set()
        self.request_delay = 5.1  # API rate limit: 1 request per 5 seconds
        self.questions_written = 0
        
        # Headers to appear more like a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Load existing questions if file exists
        self._load_existing_questions()
    
    def _load_existing_questions(self):
        """Load existing questions from CSV file to avoid duplicates and track progress"""
        if not os.path.exists(self.output_file):
            logger.info(f"No existing file found at {self.output_file}, starting fresh")
            return
        
        try:
            with open(self.output_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Recreate hash from existing question
                    question = row['question']
                    correct_answer = row['correct_answer']
                    question_hash = self._create_question_hash(question, correct_answer)
                    self.seen_hashes.add(question_hash)
                    self.questions_written += 1
            
            logger.info(f"Loaded {len(self.seen_hashes)} existing questions from {self.output_file}")
            logger.info(f"Will continue from question {self.questions_written + 1}")
            
        except Exception as e:
            logger.error(f"Error loading existing questions: {e}")
            logger.info("Starting fresh due to error reading existing file")
    
    def _make_request(self, url: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retries and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Making request to {url} (attempt {attempt + 1})")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Check API response code
                if 'response_code' in data:
                    if data['response_code'] == 0:  # Success
                        return data
                    elif data['response_code'] == 1:  # No results
                        logger.warning("No results available for this query")
                        return None
                    elif data['response_code'] == 2:  # Invalid parameter
                        logger.error("Invalid parameter in request")
                        return None
                    elif data['response_code'] == 3:  # Token not found
                        logger.warning("Session token not found, requesting new one")
                        self._get_session_token()
                        continue
                    elif data['response_code'] == 4:  # Token empty
                        logger.info("Session token exhausted, resetting")
                        self._reset_session_token()
                        continue
                    elif data['response_code'] == 5:  # Rate limit
                        logger.warning("Rate limited, waiting longer")
                        time.sleep(self.request_delay * 2)
                        continue
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
            except ValueError as e:
                logger.error(f"JSON decode error: {e}")
                return None
        
        logger.error(f"Failed to get response after {self.max_retries} attempts")
        return None
    
    def _get_session_token(self) -> bool:
        """Get a new session token"""
        params = {'command': 'request'}
        data = self._make_request(self.TOKEN_URL, params)
        
        if data and 'token' in data:
            self.session_token = data['token']
            logger.info(f"Got session token: {self.session_token[:16]}...")
            return True
        
        logger.error("Failed to get session token")
        return False
    
    def _reset_session_token(self) -> bool:
        """Reset the current session token"""
        if not self.session_token:
            return self._get_session_token()
        
        params = {'command': 'reset', 'token': self.session_token}
        data = self._make_request(self.TOKEN_URL, params)
        
        if data and data.get('response_code') == 0:
            logger.info("Session token reset successfully")
            return True
        
        logger.error("Failed to reset session token, getting new one")
        return self._get_session_token()
    
    def get_categories(self) -> List[Dict[str, Any]]:
        """Fetch all available categories"""
        logger.info("Fetching categories...")
        data = self._make_request(self.CATEGORY_URL)
        
        if data and 'trivia_categories' in data:
            categories = data['trivia_categories']
            logger.info(f"Found {len(categories)} categories")
            return categories
        
        logger.error("Failed to fetch categories")
        return []
    
    def _decode_text(self, text: str) -> str:
        """Decode HTML entities in text"""
        return html.unescape(text)
    
    def _create_question_hash(self, question: str, correct_answer: str) -> str:
        """Create a hash for the question to detect duplicates"""
        content = f"{question}:{correct_answer}".lower().strip()
        return hashlib.md5(content.encode()).hexdigest()
    
    def _write_csv_headers(self):
        """Write CSV headers if file doesn't exist"""
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['category', 'question', 'options', 'correct_answer', 'difficulty', 'type']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
            logger.info(f"Created new CSV file: {self.output_file}")
    
    def _append_question_to_csv(self, question: Question):
        """Append a single question to the CSV file"""
        try:
            self._write_csv_headers()  # Ensure headers exist
            
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['category', 'question', 'options', 'correct_answer', 'difficulty', 'type']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writerow({
                    'category': question.category,
                    'question': question.question,
                    'options': ' | '.join(question.options),
                    'correct_answer': question.correct_answer,
                    'difficulty': question.difficulty,
                    'type': question.question_type
                })
                
            self.questions_written += 1
            
            if self.questions_written % 10 == 0:  # Log progress every 10 questions
                logger.info(f"Progress: {self.questions_written} questions written to CSV")
                
        except Exception as e:
            logger.error(f"Error writing question to CSV: {e}")
    
    def _process_question(self, raw_question: Dict[str, Any], write_immediately: bool = True) -> Optional[Question]:
        """Process a raw question from the API into our format"""
        try:
            # Decode HTML entities
            category = self._decode_text(raw_question['category'])
            question = self._decode_text(raw_question['question'])
            correct_answer = self._decode_text(raw_question['correct_answer'])
            incorrect_answers = [self._decode_text(ans) for ans in raw_question['incorrect_answers']]
            
            # Create options list and shuffle
            options = [correct_answer] + incorrect_answers
            random.shuffle(options)
            
            # Create unique hash for duplicate detection
            question_hash = self._create_question_hash(question, correct_answer)
            
            # Check for duplicates
            if question_hash in self.seen_hashes:
                logger.debug(f"Duplicate question detected: {question[:50]}...")
                return None
            
            self.seen_hashes.add(question_hash)
            
            question_obj = Question(
                category=category,
                question=question,
                options=options,
                correct_answer=correct_answer,
                difficulty=raw_question['difficulty'],
                question_type=raw_question['type'],
                question_hash=question_hash
            )
            
            # Write to CSV immediately if requested
            if write_immediately:
                self._append_question_to_csv(question_obj)
            
            return question_obj
            
        except Exception as e:
            logger.error(f"Error processing question: {e}")
            return None
    
    def fetch_questions(self, amount: int = 50, category: int = None, 
                       difficulty: str = None, question_type: str = None,
                       write_immediately: bool = True) -> List[Question]:
        """Fetch questions from the API"""
        
        # Build parameters
        params = {'amount': min(amount, 50)}  # API max is 50
        
        if category:
            params['category'] = category
        if difficulty:
            params['difficulty'] = difficulty
        if question_type:
            params['type'] = question_type
        if self.session_token:
            params['token'] = self.session_token
        
        # Make request
        data = self._make_request(self.BASE_URL, params)
        
        if not data or 'results' not in data:
            return []
        
        # Process questions
        questions = []
        for raw_question in data['results']:
            processed_question = self._process_question(raw_question, write_immediately)
            if processed_question:
                questions.append(processed_question)
        
        logger.info(f"Processed {len(questions)} new questions from API")
        return questions
    
    def get_progress_info(self) -> Dict[str, Any]:
        """Get current progress information"""
        return {
            'questions_written': self.questions_written,
            'unique_hashes': len(self.seen_hashes),
            'output_file': self.output_file,
            'file_exists': os.path.exists(self.output_file)
        }
    
    def scrape_all_categories(self, questions_per_category: int = 50) -> int:
        """Scrape questions from all available categories. Returns number of new questions added."""
        
        # Get session token
        if not self._get_session_token():
            logger.warning("Proceeding without session token")
        
        # Get categories
        categories = self.get_categories()
        if not categories:
            logger.error("No categories found, cannot proceed")
            return 0
        
        initial_count = self.questions_written
        questions_added = 0
        
        for i, category in enumerate(categories):
            category_id = category['id']
            category_name = category['name']
            
            logger.info(f"Fetching from category {i+1}/{len(categories)}: {category_name}")
            
            # Fetch questions for this category (writes automatically)
            questions = self.fetch_questions(
                amount=questions_per_category,
                category=category_id,
                write_immediately=True
            )
            
            questions_added += len(questions)
            
            # Rate limiting delay
            if i < len(categories) - 1:  # Don't delay after the last category
                logger.info(f"Waiting {self.request_delay} seconds...")
                time.sleep(self.request_delay)
        
        total_written = self.questions_written - initial_count
        logger.info(f"Category scraping complete! Added {total_written} new questions")
        return total_written
    
    def scrape(self, total_questions: int = 1000, questions_per_request: int = 50) -> int:
        """Main scraping method with specified total questions. Returns number of new questions added."""
        
        # Check if we already have enough questions
        if self.questions_written >= total_questions:
            logger.info(f"Already have {self.questions_written} questions (target: {total_questions}). Nothing to do!")
            return 0
        
        # Get session token
        if not self._get_session_token():
            logger.warning("Proceeding without session token")
        
        initial_count = self.questions_written
        requests_made = 0
        
        logger.info(f"Starting scrape. Current: {self.questions_written}, Target: {total_questions}")
        
        while self.questions_written < total_questions:
            remaining = total_questions - self.questions_written
            amount = min(questions_per_request, remaining, 50)
            
            logger.info(f"Fetching {amount} questions (current total: {self.questions_written}/{total_questions})")
            
            # Fetch questions (writes automatically)
            questions = self.fetch_questions(amount=amount, write_immediately=True)
            requests_made += 1
            
            # If we got fewer questions than requested, we might be running out
            if len(questions) < amount:
                logger.warning("Received fewer questions than requested, might be running out of new questions")
                
                # If we got no new questions, break to avoid infinite loop
                if len(questions) == 0:
                    logger.warning("No new questions received, stopping scrape")
                    break
            
            # Rate limiting delay
            if self.questions_written < total_questions:
                logger.info(f"Waiting {self.request_delay} seconds...")
                time.sleep(self.request_delay)
        
        new_questions = self.questions_written - initial_count
        logger.info(f"Scraping complete! Added {new_questions} new questions in {requests_made} requests")
        logger.info(f"Total questions in file: {self.questions_written}")
        return new_questions


def main():
    """Main function to run the scraper"""
    
    # Configuration
    OUTPUT_FILE = "trivia_questions.csv"
    TOTAL_QUESTIONS = 5000  # Adjust as needed
    
    # Create scraper
    scraper = OpenTriviaDBScraper(output_file=OUTPUT_FILE)
    
    try:
        # Show current progress
        progress = scraper.get_progress_info()
        logger.info(f"Current progress: {progress}")
        
        if progress['questions_written'] >= TOTAL_QUESTIONS:
            logger.info(f"Target of {TOTAL_QUESTIONS} questions already reached!")
            return
        
        # Option 1: Scrape a specific number of questions
        logger.info(f"Starting to scrape up to {TOTAL_QUESTIONS} questions...")
        new_questions = scraper.scrape(total_questions=TOTAL_QUESTIONS)
        
        # Option 2: Alternatively, scrape from all categories
        # logger.info("Starting to scrape from all categories...")
        # new_questions = scraper.scrape_all_categories(questions_per_category=25)
        
        if new_questions > 0:
            logger.info(f"Scraping completed successfully! Added {new_questions} new questions")
            
            # Print some statistics by reading the CSV file
            try:
                categories = set()
                difficulties = {}
                total_questions = 0
                
                with open(OUTPUT_FILE, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        categories.add(row['category'])
                        difficulty = row['difficulty']
                        difficulties[difficulty] = difficulties.get(difficulty, 0) + 1
                        total_questions += 1
                
                logger.info(f"Final Statistics:")
                logger.info(f"  Total questions in file: {total_questions}")
                logger.info(f"  Unique categories: {len(categories)}")
                logger.info(f"  Difficulty breakdown: {difficulties}")
                
            except Exception as e:
                logger.error(f"Error reading final statistics: {e}")
            
        else:
            logger.info("No new questions were added (may have reached API limits or all questions already scraped)")
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        progress = scraper.get_progress_info()
        logger.info(f"Progress saved: {progress['questions_written']} questions written to {OUTPUT_FILE}")
        logger.info("You can resume by running the script again")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        progress = scraper.get_progress_info()
        logger.info(f"Progress saved: {progress['questions_written']} questions written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()