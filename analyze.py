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
        'question_types': Counter(),
        'difficulties': Counter(),
        'sample_questions': []
    })
    
    total_questions = 0
    all_difficulties = Counter()
    all_question_types = Counter()
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                category = row['category']
                question_type = row['type']
                difficulty = row['difficulty']
                question = row['question']
                
                # Update counters
                total_questions += 1
                categories[category]['total_questions'] += 1
                categories[category]['question_types'][question_type] += 1
                categories[category]['difficulties'][difficulty] += 1
                
                # Store sample questions (max 3 per category)
                if len(categories[category]['sample_questions']) < 3:
                    categories[category]['sample_questions'].append(question[:100] + "..." if len(question) > 100 else question)
                
                # Global counters
                all_difficulties[difficulty] += 1
                all_question_types[question_type] += 1
        
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
    
    print(f"\n📝 Question Types (Overall):")
    for q_type, count in all_question_types.most_common():
        percentage = (count / total_questions) * 100
        print(f"  • {q_type.title()}: {count:,} ({percentage:.1f}%)")
    
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
        
        # Question types for this category
        print(f"   📝 Question Types:")
        for q_type, count in data['question_types'].most_common():
            type_percentage = (count / data['total_questions']) * 100
            print(f"      • {q_type.title()}: {count} ({type_percentage:.1f}%)")
        
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
    print(f"{'Category':<35} {'Questions':<10} {'Multiple':<8} {'Boolean':<7} {'Easy':<5} {'Medium':<6} {'Hard':<4}")
    print("-" * 80)
    
    for category, data in sorted_categories:
        multiple_count = data['question_types'].get('multiple', 0)
        boolean_count = data['question_types'].get('boolean', 0)
        easy_count = data['difficulties'].get('easy', 0)
        medium_count = data['difficulties'].get('medium', 0)
        hard_count = data['difficulties'].get('hard', 0)
        
        # Truncate long category names
        display_category = category[:32] + "..." if len(category) > 32 else category
        
        print(f"{display_category:<35} {data['total_questions']:<10} {multiple_count:<8} {boolean_count:<7} {easy_count:<5} {medium_count:<6} {hard_count:<4}")
    
    # Top categories
    print(f"\n🏆 TOP 10 CATEGORIES BY QUESTION COUNT")
    print("=" * 50)
    for i, (category, data) in enumerate(sorted_categories[:10], 1):
        percentage = (data['total_questions'] / total_questions) * 100
        print(f"{i:2d}. {category:<40} {data['total_questions']:>4} questions ({percentage:4.1f}%)")
    
    # Distribution insights
    print(f"\n💡 INSIGHTS")
    print("=" * 50)
    
    # Find categories with most/least questions
    most_questions_cat, most_questions_data = sorted_categories[0]
    least_questions_cat, least_questions_data = sorted_categories[-1]
    
    print(f"• Most questions: {most_questions_cat} ({most_questions_data['total_questions']} questions)")
    print(f"• Least questions: {least_questions_cat} ({least_questions_data['total_questions']} questions)")
    
    # Find categories with only one type of question
    single_type_categories = []
    for category, data in categories.items():
        if len(data['question_types']) == 1:
            single_type_categories.append((category, list(data['question_types'].keys())[0]))
    
    if single_type_categories:
        print(f"• Categories with single question type ({len(single_type_categories)}):")
        for cat, q_type in single_type_categories[:5]:  # Show first 5
            print(f"  - {cat}: only {q_type}")
        if len(single_type_categories) > 5:
            print(f"  ... and {len(single_type_categories) - 5} more")
    
    # Average questions per category
    avg_questions = total_questions / len(categories)
    print(f"• Average questions per category: {avg_questions:.1f}")
    
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