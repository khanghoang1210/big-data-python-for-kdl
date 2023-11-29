import pandas as pd


def process_data(input_path, output_path):
    # Đọc dữ liệu từ file CSV
    data = pd.read_csv(input_path)

    # Xử lý cột 'REVENUE'
    data['REVENUE'] = data['REVENUE'].str.replace(',', '').astype(float)

    # Thay thế giá trị '-' bằng 0 trong cột 'GROSS_CHANGE_PER_DAY' và 'GROSS_CHANGE_PER_WEEK'
    data['GROSS_CHANGE_PER_DAY'] = data['GROSS_CHANGE_PER_DAY'].replace('-', '0')
    data['GROSS_CHANGE_PER_WEEK'] = data['GROSS_CHANGE_PER_WEEK'].replace('-', '0')

    # Xử lý cột 'GROSS_CHANGE_PER_DAY'
    data['GROSS_CHANGE_PER_DAY'] = data['GROSS_CHANGE_PER_DAY'].str.replace('%', '').str.replace(',', '').astype(float)
    data['GROSS_CHANGE_PER_DAY'].fillna(0, inplace=True)

    # Xử lý cột 'GROSS_CHANGE_PER_WEEK'
    data['GROSS_CHANGE_PER_WEEK'] = data['GROSS_CHANGE_PER_WEEK'].str.replace('%', '').str.replace(',', '').astype(
        float)
    data['GROSS_CHANGE_PER_WEEK'].fillna(0, inplace=True)


def process_movie_data(input_path, output_path):
    # Read data from CSV file
    data = pd.read_csv(input_path)

    # Process 'RATING' column: Convert to float
    data['RATING'] = data['RATING'].astype(str).str.extract('(\d+\.?\d*)').astype(float)

    # Function to convert monetary values
    def convert_monetary_values(value):
        if pd.isna(value):
            return 20e6  # Default value for nulls
        value = value.replace(' million', '').replace('$', '').replace(',', '')
        try:
            return float(value) * 1e6 if 'million' in value else float(value)
        except ValueError:
            return 0.0

    # Process 'BUDGET' and 'WORLDWIDE_GROSS' columns
    data['BUDGET'] = data['BUDGET'].apply(convert_monetary_values)
    data['WORLDWIDE_GROSS'] = data['WORLDWIDE_GROSS'].apply(convert_monetary_values)

    # Process 'GENRE' column: Handling specific text patterns
    data['GENRE'] = data['GENRE'].str.replace('Its Me, Margaret.?', '', regex=True)