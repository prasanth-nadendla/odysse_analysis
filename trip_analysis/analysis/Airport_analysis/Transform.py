import pandas as pd
import re

def add_datetime_features(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds datetime-based features such as report date, hour, month, etc.
    """
    data['triprequestdatetime'] = pd.to_datetime(data['triprequestdatetime'])
    data['tridroppffdatetime'] = pd.to_datetime(data['tridroppffdatetime'])
    
    data['Report_date'] = data['triprequestdatetime'].dt.date
    data['Hour'] = data['triprequestdatetime'].dt.hour
    data['Month'] = data['triprequestdatetime'].dt.month
    data['Minute'] = data['triprequestdatetime'].dt.minute
    data['Day'] = data['triprequestdatetime'].dt.day
    data['pickup_time'] = data['triprequestdatetime'].dt.time
    data['dropoff_time'] = data['tridroppffdatetime'].dt.time
    
    data['Weekday'] = data['triprequestdatetime'].dt.dayofweek < 4
    data['Weekend'] = ~data['Weekday']

    def get_time_of_day(hour):
        if 4 <= hour < 8:
            return 'Early Morning'
        elif 8 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 16:
            return 'Afternoon'
        elif 16 <= hour < 20:
            return 'Evening'
        elif 20 <= hour < 24:
            return 'Night'
        else:
            return 'Late Night'
    
    data['TimeOfDay'] = data['Hour'].apply(get_time_of_day)
    
    return data

def extract_postal_code(address):
    """
    Extracts postal and street codes from address strings.
    """
    if address is None or not isinstance(address, str):
        return None, None

    prefixes = ['E', 'EC', 'N', 'NW', 'SE', 'SW', 'W', 'WC', 'BR', 'CM', 'CR', 'DA', 'EN', 'HA', 'IG', 'SL', 'TN', 'KT',
                'RM', 'SM', 'TW', 'UB', 'WD', 'LU', 'RH']
    pattern = r'\b(?:' + '|'.join(prefixes) + r')\d+[A-Z]?\b'

    match = re.search(pattern, address)

    if match:
        post_code = match.group(0)
        parts = address.split(post_code)
        if len(parts) > 1:
            street_parts = parts[1].split(',')
            return post_code, street_parts[0].strip() if street_parts else None
        return post_code, None
    return None, None

def add_postal_street_codes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds pickup and drop-off postal and street codes.
    """
    data['Pick_up_postcode'], data['Pick_up_streetcode'] = zip(*data['pickupaddress'].apply(extract_postal_code))
    data['Drop_off_postcode'], data['Drop_off_streetcode'] = zip(*data['dropoffaddress'].apply(extract_postal_code))
    return data

def add_day_of_week(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds day of the week and day name based on report date.
    """
    data['Report_date'] = pd.to_datetime(data['Report_date'])
    data['Day_of_week'] = data['Report_date'].dt.dayofweek

    day_mapping = {
        0: 'Monday',
        1: 'Tuesday',
        2: 'Wednesday',
        3: 'Thursday',
        4: 'Friday',
        5: 'Saturday',
        6: 'Sunday'
    }
    
    data['Day_name'] = data['Day_of_week'].map(day_mapping)
    return data

def add_trip_sequence_info(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds next and previous pickup/dropoff times, postcodes, street codes, and addresses for each trip.
    """
    data = data.sort_values(['Report_date', 'full_name', 'pickup_time'])

    data['next_pickup_time'] = data.groupby(['Report_date', 'full_name'])['pickup_time'].shift(-1)
    data['previous_dropoff_time'] = data.groupby(['Report_date', 'full_name'])['dropoff_time'].shift(1)

    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmin(), 'previous_dropoff_time'] = 'first trip of day'
    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmax(), 'next_pickup_time'] = 'last trip of day'

    data['next_pickup_postcode'] = data.groupby(['Report_date', 'full_name'])['Pick_up_postcode'].shift(-1)
    data['previous_dropoff_postcode'] = data.groupby(['Report_date', 'full_name'])['Drop_off_postcode'].shift(1)

    data['next_pickup_streetcode'] = data.groupby(['Report_date', 'full_name'])['Pick_up_streetcode'].shift(-1)
    data['previous_dropoff_streetcode'] = data.groupby(['Report_date', 'full_name'])['Drop_off_streetcode'].shift(1)

    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmin(), 'previous_dropoff_postcode'] = 'first trip of day'
    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmax(), 'next_pickup_postcode'] = 'last trip of day'

    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmin(), 'previous_dropoff_streetcode'] = 'first trip of day'
    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmax(), 'next_pickup_streetcode'] = 'last trip of day'

    data['next_pickup_address'] = data.groupby(['Report_date', 'full_name'])['pickupaddress'].shift(-1)
    data['previous_dropoff_address'] = data.groupby(['Report_date', 'full_name'])['dropoffaddress'].shift(1)

    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmin(), 'previous_dropoff_address'] = 'first trip of day'
    data.loc[data.groupby(['Report_date', 'full_name'])['pickup_time'].idxmax(), 'next_pickup_address'] = 'last trip of day'
    
    return data

def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Run all transformations and return the transformed DataFrame.
    """
    data = add_datetime_features(data)
    data = add_postal_street_codes(data)
    data = add_day_of_week(data)
    data = add_trip_sequence_info(data)
    print("Data transformation completed for airport analysis.")
    return data
