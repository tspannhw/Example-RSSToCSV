import feedparser
import pandas as pd
import io
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators


class RSSToCSV(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        dependencies = ['feedparser==6.0.11', 'pandas==2.2.0']
        description = 'Reads an RSS feed from a URL and converts it to CSV format'
        tags = ['rss', 'csv', 'feed', 'transform']

    def __init__(self, **kwargs):
        # Call parent constructor to handle any NiFi-specific initialization
        super().__init__()
        
        # Define properties
        self.rss_url = PropertyDescriptor(
            name='RSS URL',
            description='The URL of the RSS feed to read',
            required=True,
            validators=[StandardValidators.URL_VALIDATOR]
        )

    def getPropertyDescriptors(self):
        return [self.rss_url]

    def transform(self, context, flowfile):
        try:
            # Get the RSS URL from properties
            rss_url = context.getProperty(self.rss_url).getValue()
            
            # Parse the RSS feed
            feed = feedparser.parse(rss_url)
            
            # Check if feed was parsed successfully
            if hasattr(feed, 'bozo') and feed.bozo:
                return FlowFileTransformResult(
                    relationship='failure',
                    attributes={'error': 'Failed to parse RSS feed'}
                )
            
            # Extract feed items data
            items = []
            for entry in feed.entries:
                item = {
                    'title': getattr(entry, 'title', ''),
                    'link': getattr(entry, 'link', ''),
                    'description': getattr(entry, 'description', ''),
                    'published': getattr(entry, 'published', ''),
                    'author': getattr(entry, 'author', ''),
                    'category': ', '.join([tag.term for tag in getattr(entry, 'tags', [])]) if hasattr(entry, 'tags') else ''
                }
                items.append(item)
            
            # Convert to DataFrame and then to CSV
            df = pd.DataFrame(items)
            
            # Convert DataFrame to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Update flowfile attributes
            attributes = {
                'mime.type': 'text/csv',
                'filename': f'rss_feed_{feed.feed.get("title", "unknown").replace(" ", "_")}.csv',
                'rss.feed.title': feed.feed.get('title', ''),
                'rss.feed.description': feed.feed.get('description', ''),
                'rss.item.count': str(len(items))
            }
            
            return FlowFileTransformResult(
                relationship='success',
                contents=csv_content,
                attributes=attributes
            )
            
        except Exception as e:
            return FlowFileTransformResult(
                relationship='failure',
                attributes={'error': str(e)}
            )

    def getRelationships(self):
        return [
            {
                'name': 'success',
                'description': 'FlowFiles that are successfully converted to CSV'
            },
            {
                'name': 'failure', 
                'description': 'FlowFiles that failed to be processed'
            }
        ]
