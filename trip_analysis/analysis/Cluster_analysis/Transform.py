import pandas as pd
import re
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors


def lat_long_cleaning(degree):
    """
    Cleans and converts DMS (Degrees, Minutes, Seconds) coordinates into decimal degrees.
    """
    degree = degree.replace("\\", "").replace("Â", "").replace("�", "°").strip()
    try:
        match = re.match(r"(\d+)°\s*(\d+)'?\s*(\d+\.?\d*)?\"?\s*([NSEW])?", degree)
        if not match:
            raise ValueError(f"Invalid format: {degree}")
        
        deg, minutes, seconds, direction = match.groups()
        deg = float(deg)
        minutes = float(minutes) if minutes else 0
        seconds = float(seconds) if seconds else 0

        # Normalize seconds and minutes
        if seconds >= 60:
            minutes += int(seconds // 60)
            seconds %= 60
        if minutes >= 60:
            deg += int(minutes // 60)
            minutes %= 60

        decimal = deg + (minutes / 60) + (seconds / 3600)
        if direction in ['S', 'W']:
            decimal *= -1

        return decimal
    except Exception as e:
        raise ValueError(f"Error cleaning latitude/longitude '{degree}': {e}")


def validate_columns(data, required_columns):
    """
    Validates that the DataFrame contains required columns.
    """
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    return True


def apply_dbscan_clustering(data: pd.DataFrame) -> pd.DataFrame:
    """
    Applies DBSCAN clustering to pickup coordinates and assigns dropoff clusters based on nearest pickups.
    """
    validate_columns(data, ['start_latitude', 'start_longitude', 'end_latitude', 'end_longitude'])

    data['start_lat_dms'] = data['start_latitude'].apply(lat_long_cleaning)
    data['start_long_dms'] = data['start_longitude'].apply(lat_long_cleaning)
    data['end_lat_dms'] = data['end_latitude'].apply(lat_long_cleaning)
    data['end_long_dms'] = data['end_longitude'].apply(lat_long_cleaning)

    pickup_coords = data[['start_lat_dms', 'start_long_dms']].values

    dbs = DBSCAN(eps=0.05, min_samples=20, algorithm='ball_tree').fit(pickup_coords)
    data['Pickup_Cluster'] = dbs.labels_

    dropoff_coords = data[['end_lat_dms', 'end_long_dms']].values
    nearest_pickup_model = NearestNeighbors(n_neighbors=1, algorithm='ball_tree').fit(pickup_coords)

    distances, indices = nearest_pickup_model.kneighbors(dropoff_coords)
    data['Dropoff_Cluster'] = data.iloc[indices.flatten()]['Pickup_Cluster'].values
    data.drop(columns=['start_lat_dms', 'start_long_dms', 'end_lat_dms', 'end_long_dms'], inplace=True)
    print("Clustering complete.")
    return data


def add_datetime_features(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds datetime-based features such as report date, hour, month, etc.
    """
    validate_columns(data, ['triprequestdatetime', 'tridroppffdatetime'])
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
    print("Datetime features added.")
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
        lis = address.split(post_code)
        if len(lis) > 1:
            st_lis = lis[1].split(',')
            return post_code, st_lis[0].strip() if len(st_lis) > 1 else lis[1].strip()
        else:
            return post_code, None
    else:
        return None, None


def add_postal_street_codes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds pickup and drop-off postal and street codes.
    """
    validate_columns(data, ['pickupaddress', 'dropoffaddress'])
    data['Pick_up_postcode'], data['Pick_up_streetcode'] = zip(*data['pickupaddress'].apply(extract_postal_code))
    data['Drop_off_postcode'], data['Drop_off_streetcode'] = zip(*data['dropoffaddress'].apply(extract_postal_code))
    print("Postal and street codes added.")
    return data


def add_day_of_week(data: pd.DataFrame) -> pd.DataFrame:
    """
    Adds day of week and day name based on report date.
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
    print("Day of week added.")
    return data


def transform_data_cluster(data: pd.DataFrame) -> pd.DataFrame:
    """
    Run all transformations and return transformed DataFrame.
    """
    print("Starting data transformation process...")
    data = apply_dbscan_clustering(data)
    data = add_datetime_features(data)
    data = add_postal_street_codes(data)
    data = add_day_of_week(data)
    print("Data transformation completed.")
    return data
