# Connecting cleaned data to metadata from Wikipedia based on keywords in trivia questions

Enables automatic categorization of trivia questions (works reasonably well, much better than no category) and potential to use Wikipedia category info in search functionality.

### Comments
See folders final-notebooks and source-data for workflow.

Identify keywords from trivia question; Connect to corresponding Wikipedia article; Identify article category parents; Calculate strongest category path from article to Wikipedia's main topics; Deduce trivia category.

Final files (in local folder final-files) added to google drive: https://drive.google.com/drive/folders/1OxthJsxZk7eNreNTQ9PtHFepQixJv-h1?usp=sharing.
final-files and workproduct-files directories in gitignore.

### Meta data linking can be improved by:
- Rerun categorization script on first ~10k rows (or more, not sure). Keywords were improved during process.
- Run script with longer max allowed time per keyword.
- Run script on more keywords per row