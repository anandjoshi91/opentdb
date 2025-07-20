#!/usr/bin/env python3
"""
Trivia Data Analyzer

Analyzes the scraped trivia questions CSV file and provides comprehensive statistics.
"""

import csv
import os
from collections import defaultdict, Counter
from typing import Dict, List, Any

def analyze_trivia_data(csv_file: str = "trivia_questions.csv"):
    """Analyze trivia data and print comprehensive statistics"""
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: File '{csv_file}' not found!")
        print("Make sure the CSV file is in the same directory as this script.")
        return
    
    # Data structures to store analysis
    categories = defaultdict(lambda: {
        'total_questions': 0,
        'difficulties': Counter(),
        'sample_questions': []
    })
    
    total_questions = 0
    all_difficulties = Counter()
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Verify expected columns exist
            expected_columns = {'id', 'category', 'question', 'options', 'correct_answer', 'difficulty'}
            if not expected_columns.issubset(set(reader.fieldnames)):
                missing = expected_columns - set(reader.fieldnames)
                print(f"❌ Error: Missing expected columns: {missing}")
                print(f"Found columns: {reader.fieldnames}")
                return
            
            for row in reader:
                category = row['category']
                difficulty = row['difficulty']
                question = row['question']
                
                # Update counters
                total_questions += 1
                categories[category]['total_questions'] += 1
                categories[category]['difficulties'][difficulty] += 1
                
                # Store sample questions (max 3 per category)
                if len(categories[category]['sample_questions']) < 3:
                    categories[category]['sample_questions'].append(question[:100] + "..." if len(question) > 100 else question)
                
                # Global counters
                all_difficulties[difficulty] += 1
        
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return
    
    # Print analysis results
    print("🎯 TRIVIA QUESTIONS ANALYSIS")
    print("=" * 50)
    
    # Overall statistics
    print(f"\n📊 OVERALL STATISTICS")
    print(f"Total Questions: {total_questions:,}")
    print(f"Total Categories: {len(categories)}")
    
    print(f"\n⭐ Difficulty Levels (Overall):")
    for difficulty, count in all_difficulties.most_common():
        percentage = (count / total_questions) * 100
        print(f"  • {difficulty.title()}: {count:,} ({percentage:.1f}%)")
    
    # Category breakdown
    print(f"\n📚 DETAILED CATEGORY BREAKDOWN")
    print("=" * 50)
    
    # Sort categories by number of questions (descending)
    sorted_categories = sorted(categories.items(), key=lambda x: x[1]['total_questions'], reverse=True)
    
    for i, (category, data) in enumerate(sorted_categories, 1):
        percentage = (data['total_questions'] / total_questions) * 100
        print(f"\n{i}. {category}")
        print(f"   📊 Questions: {data['total_questions']:,} ({percentage:.1f}% of total)")
        
        # Difficulties for this category
        print(f"   ⭐ Difficulties:")
        for difficulty, count in data['difficulties'].most_common():
            diff_percentage = (count / data['total_questions']) * 100
            print(f"      • {difficulty.title()}: {count} ({diff_percentage:.1f}%)")
        
        # Sample questions
        if data['sample_questions']:
            print(f"   💡 Sample Questions:")
            for j, sample in enumerate(data['sample_questions'], 1):
                print(f"      {j}. {sample}")
    
    # Summary table
    print(f"\n📋 QUICK SUMMARY TABLE")
    print("=" * 50)
    print(f"{'Category':<40} {'Questions':<10} {'Easy':<5} {'Medium':<6} {'Hard':<4}")
    print("-" * 70)
    
    for category, data in sorted_categories:
        easy_count = data['difficulties'].get('easy', 0)
        medium_count = data['difficulties'].get('medium', 0)
        hard_count = data['difficulties'].get('hard', 0)
        
        # Truncate long category names
        display_category = category[:37] + "..." if len(category) > 37 else category
        
        print(f"{display_category:<40} {data['total_questions']:<10} {easy_count:<5} {medium_count:<6} {hard_count:<4}")
    
    # Top categories
    print(f"\n🏆 TOP 10 CATEGORIES BY QUESTION COUNT")
    print("=" * 50)
    for i, (category, data) in enumerate(sorted_categories[:10], 1):
        percentage = (data['total_questions'] / total_questions) * 100
        print(f"{i:2d}. {category:<45} {data['total_questions']:>4} questions ({percentage:4.1f}%)")
    
    # Distribution insights
    print(f"\n💡 INSIGHTS")
    print("=" * 50)
    
    # Find categories with most/least questions
    most_questions_cat, most_questions_data = sorted_categories[0]
    least_questions_cat, least_questions_data = sorted_categories[-1]
    
    print(f"• Most questions: {most_questions_cat} ({most_questions_data['total_questions']} questions)")
    print(f"• Least questions: {least_questions_cat} ({least_questions_data['total_questions']} questions)")
    
    # Find difficulty distribution insights
    easiest_categories = []
    hardest_categories = []
    
    for category, data in categories.items():
        total_cat_questions = data['total_questions']
        if total_cat_questions >= 10:  # Only consider categories with at least 10 questions
            easy_percentage = (data['difficulties'].get('easy', 0) / total_cat_questions) * 100
            hard_percentage = (data['difficulties'].get('hard', 0) / total_cat_questions) * 100
            
            if easy_percentage >= 60:  # 60% or more easy questions
                easiest_categories.append((category, easy_percentage))
            if hard_percentage >= 40:  # 40% or more hard questions
                hardest_categories.append((category, hard_percentage))
    
    if easiest_categories:
        easiest_categories.sort(key=lambda x: x[1], reverse=True)
        print(f"• Categories with mostly easy questions ({len(easiest_categories)}):")
        for cat, percentage in easiest_categories[:3]:  # Show top 3
            print(f"  - {cat}: {percentage:.1f}% easy")
    
    if hardest_categories:
        hardest_categories.sort(key=lambda x: x[1], reverse=True)
        print(f"• Categories with many hard questions ({len(hardest_categories)}):")
        for cat, percentage in hardest_categories[:3]:  # Show top 3
            print(f"  - {cat}: {percentage:.1f}% hard")
    
    # Average questions per category
    avg_questions = total_questions / len(categories)
    print(f"• Average questions per category: {avg_questions:.1f}")
    
    # Show ID range
    print(f"• Question IDs range from 1 to {total_questions}")
    
    print(f"\n✅ Analysis complete! Data from '{csv_file}' successfully analyzed.")


def main():
    """Main function"""
    import sys
    
    # Allow custom CSV file path as command line argument
    csv_file = "trivia_questions.csv"
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    
    print("🎯 Trivia Questions Data Analyzer")
    print(f"📁 Analyzing file: {csv_file}")
    print()
    
    analyze_trivia_data(csv_file)


if __name__ == "__main__":
    main()