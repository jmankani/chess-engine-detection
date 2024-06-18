import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import time
import logging

# Create string with current date and time
current_time = time.strftime("%Y-%m-%d-%H-%M-%S")

# Set log file path
log_file = os.path.join('logs', f'{current_time}_visualizations.log')

# Set up logging to file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file)

def load_data(filepath):
    """Load data from a CSV file and handle errors."""
    if os.path.exists(filepath):
        return pd.read_csv(filepath)
    else:
        raise FileNotFoundError(f"No file found at {filepath}")

def process_data(df):
    """Process DataFrame to prepare data for visualizations."""
    elo_bins = pd.cut(df['white_elo'], bins=[999, 1400, 1800, 2200, 2600, 3000], labels=['1000-1399', '1400-1799', '1800-2199', '2200-2599', '2600-2999'])
    percent_bins = pd.cut(df['white_engine_move_percentage'], bins=[0, 0.2, 0.4, 0.6, 0.80, 1.00], labels=['0-20%', '21-40%', '41-60%', '61-80%', '81-100%'])
    
    heatmap_data = pd.DataFrame({
        'Elo Range': elo_bins,
        'Engine Move Percentage Range': percent_bins,
        'Result': df['result']
    })
    return heatmap_data

def create_heatmap(heatmap_data):
    """Create and save a heatmap."""
    pivot_table = pd.pivot_table(heatmap_data, values='Result', index=['Elo Range'], columns=['Engine Move Percentage Range'], aggfunc='size', fill_value=0)
    normalized_pivot = pivot_table.div(pivot_table.sum(axis=1) + 1e-9, axis=0)  # Added a small constant to avoid division by zero
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(normalized_pivot, annot=True, fmt=".2f", cmap='coolwarm')
    plt.title('Heatmap of Engine Move Percentage by Elo and Result')
    plt.ylabel('Elo Range')
    plt.xlabel('Engine Move Percentage Range')
    plt.savefig('results/plots/heatmap.png')
    plt.show()

def plot_scatter_plots(df):
    """Create and save scatter plots for white and black player data."""
    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1) 
    sns.scatterplot(x='white_elo', y='white_engine_move_percentage', hue='result', data=df, palette='coolwarm', style='result', markers=True)
    plt.title('White Elo vs. Engine Move Percentage')
    plt.xlabel('White Elo')
    plt.ylabel('Engine Move Percentage')

    plt.subplot(1, 2, 2)
    sns.scatterplot(x='black_elo', y='black_engine_move_percentage', hue='result', data=df, palette='coolwarm', style='result', markers=True)
    plt.title('Black Elo vs. Engine Move Percentage')
    plt.xlabel('Black Elo')
    plt.ylabel('Engine Move Percentage')

    plt.tight_layout()
    plt.savefig('results/plots/scatterplot.png')
    plt.show()

def plot_bubble_chart(agg_df):
    """Create and save a bubble chart based on player statistics."""
    plt.figure(figsize=(10, 6))
    bubble_sizes = agg_df['game_volume'] * 10
    
    for i, row in agg_df.iterrows():
        plt.scatter(row['elo'], row['engine_move_percent'], s=bubble_sizes[i], alpha=0.5, label=f"{row['user']} ({row['game_volume']} games)")

    plt.xlabel('Average Elo Rating')
    plt.ylabel('Average Engine Move Percentage')
    plt.title('Bubble Chart of Elo, Engine Move Percentage, and Game Volume')
    plt.legend(title="User (Game Volume)")
    plt.grid(True)
    plt.savefig('results/plots/bubble_chart.png')
    plt.show()

def main():
    """Main function to orchestrate data loading, processing, and visualization."""
    try:
        df = load_data('results/gamewise_engine_move_percentages.csv')
        heatmap_data = process_data(df)
        create_heatmap(heatmap_data)
        plot_scatter_plots(df)
        
        df_user = load_data('results/userwise_game_data.csv')
        agg_df = df_user.groupby('user').agg({
            'elo': 'mean',
            'engine_move_percent': 'mean',
            'date': 'count'
        }).reset_index()
        agg_df.rename(columns={'date': 'game_volume'}, inplace=True)
        plot_bubble_chart(agg_df)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        
if __name__ == "__main__":
    main()
