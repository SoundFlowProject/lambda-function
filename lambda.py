import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import boto3
import pandas as pd

nltk.data.path.append("/opt/nltk_data")

# Initialize the Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

 # Replace with your S3 bucket and object information
bucket_name = 'soundflow-songs-bucket'
object_key = 'data.csv'

# Create an S3 client
s3_client = boto3.client('s3')

# Retrieve the contents of the S3 object
response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

initial_df = pd.read_csv(response.get('Body')) # 'Body' is a key word

def lambda_handler(event, context):
    

    try:
        # Get the 'text' value from the event
        text_value = event['text']
    
        if text_value is not None:
            sentiment_score = sia.polarity_scores(text_value)['compound']
    
            normalized_score = (sentiment_score + 1) / 2
    
        else:
            return {
                'statusCode': 400,
                'message': 'No text value provided in the request'
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error score': str(e)})
        }
    
    try:

        filtered_df = pd.DataFrame()
        increment = [i / 1000 for i in range(1, 1000, 1)]
    
        for inc in increment:
            filtered_df = initial_df[initial_df['valence'].between(normalized_score - inc, normalized_score + inc)]
    
            if filtered_df.size > 0:
                break
    
        selected_row = filtered_df.sample()
        spotify_id = selected_row['id'].iloc[0]
    
        return {
            'statusCode': 200,
            'body': json.dumps({'spotifyId': spotify_id})
        }
        
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error bucket': str(e)})
        }
        


