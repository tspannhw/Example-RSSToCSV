# Apache NiFi Python Processors

This repository contains custom Apache NiFi Python processors.

## Requirements

- Apache NiFi 2.5.0 or later
- Python 3.8 or later

## Installation

1. Copy the `.py` processor files to your NiFi Python extensions directory:
   ```
   $NIFI_HOME/python/extensions/
   ```

2. The dependencies will be automatically installed by NiFi when the processor is first used.

## Processors

### RSSToCSV

Reads an RSS feed from a specified URL and converts it to CSV format.

**Properties:**
- **RSS URL** (required): The URL of the RSS feed to read

**Relationships:**
- **success**: FlowFiles that are successfully converted to CSV
- **failure**: FlowFiles that failed to be processed

**Output:**
- CSV file containing RSS feed items with columns: title, link, description, published, author, category
- FlowFile attributes include feed metadata and item count

**Example Usage:**
1. Add the RSSToCSV processor to your flow
2. Configure the RSS URL property (e.g., `https://feeds.bbci.co.uk/news/rss.xml`)
3. Connect the success relationship to downstream processors
4. Connect the failure relationship for error handling

## Dependencies

Dependencies are automatically managed by NiFi using the package-level dependencies specified in each processor:

- `feedparser==6.0.11` - RSS feed parsing
- `pandas==2.2.0` - Data manipulation and CSV generation
