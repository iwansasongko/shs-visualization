import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def create_gpu_visualization(data_df):
    plt.figure(figsize=(15, 8))

    sns.barplot(data=data_df, x='year', y='avg_usage', hue='name')

    plt.title('Top 5 Most Frequent GPUs Usage by Year', fontsize=14, pad=20)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Average Usage (%)', fontsize=12)

    plt.xticks(rotation=45, ha='right')

    plt.legend(title='GPU Name', bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()

    plt.savefig('gpu_usage_trends.png', bbox_inches='tight', dpi=300)
    plt.close()

if __name__ == "__main__":
    data_df = pd.read_parquet("output/part-00000-dafc6db8-b54b-42e5-bef0-91de08a7cd9d-c000.snappy.parquet")
    create_gpu_visualization(data_df)