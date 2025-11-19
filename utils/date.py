"""Create a function that returns the first day of the current month and the actual as a string in 'YYYY-MM-DD' format."""
from datetime import datetime
def get_month_start_and_today():
    """Returns the first day of the current month and today's date as strings in 'YYYY-MM-DD' format."""
    today = datetime.today()
    month_start = today.replace(day=1)
    return month_start.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')