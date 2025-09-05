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

    rss_url = PropertyDescriptor(
            name='RSS URL',
            description='The URL of the RSS feed to read',
            required=True,
            validators=[StandardValidators.URL_VALIDATOR]
    )

    property_descriptors = [
        rss_url
    ]

    def __init__(self, **kwargs):
        super().__init__()
        self.property_descriptors.append(self.rss_url)

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        import feedparser
        import pandas as pd
        try:
            # Get the RSS URL from properties
            rss_url = context.getProperty(self.rss_url).getValue()
            
            # Parse the RSS feed
            feed = feedparser.parse(rss_url)
            
            # Check if feed was parsed successfully and has entries
            if hasattr(feed, 'bozo') and feed.bozo and not feed.entries:
                error_msg = f"Failed to parse RSS feed. Bozo exception: {getattr(feed, 'bozo_exception', 'Unknown error')}"
                return FlowFileTransformResult(
                    relationship="failure",
                    attributes={'error': error_msg}
                )
            
            # Check if feed has entries
            if not feed.entries:
                return FlowFileTransformResult(
                    relationship="failure",
                    attributes={'error': 'RSS feed contains no entries'}
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
                relationship="success",
                contents=csv_content,
                attributes=attributes
            )
        except Exception as e:
            import traceback
            error_details = {
                'error': str(e),
                'error_type': type(e).__name__,
                'rss_url': context.getProperty(self.rss_url).getValue() if context.getProperty(self.rss_url) else 'Unknown',
                'traceback': traceback.format_exc()
            }
            return FlowFileTransformResult(
                relationship="failure",
                attributes=error_details
            )
