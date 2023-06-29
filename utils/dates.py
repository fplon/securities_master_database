from datetime import datetime, timedelta

def get_prev_bd(dt: str) -> str:

    """

    Args:
        dt (str): date in the format (yyyymmdd)

    Returns:
       prev_bd_dt (str): prev business date as string (yyyymmdd)

    """


    dt = datetime.strptime(dt, '%Y-%m-%d')
    if dt.weekday() == 0:
        dt_diff = 3
    elif dt.weekday() == 6:
        dt_diff = 2
    else:
        dt_diff = 1

    prev_bd_dt = (dt - timedelta(dt_diff)).strftime('%Y-%m-%d')

    return prev_bd_dt