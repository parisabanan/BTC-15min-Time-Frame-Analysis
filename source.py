import pandas as pd
import pandas_ta as pta
from datetime import datetime
import warnings

warnings.simplefilter('ignore')

dataframe = pd.read_csv('BTC.csv')
down_trends = pd.DataFrame(columns=['start_date', 'end_date', 'number_of_candles', 'price_change', 'percentage'])
up_trends = pd.DataFrame(columns=['start_date', 'end_date', 'number_of_candles', 'price_change', 'percentage'])


def preprocess_dataframe(df):
    """
    This function converts dataset's timestamp to a human-readable format and then selects last year's data.
    """
    df['time'] = ''
    times = []
    for time in df['timestamp']:
        times.append(datetime.fromtimestamp(time).strftime("%b %d %Y %H:%M:%S"))
    df['time'] = times
    df = df.tail(35040).reset_index()    # Select last year's data
    del df['timestamp']
    del df['index']
    return df


def calculate_rsi(df, time_period=14):
    """
    This function calculates dataset's RSI based on close price and in 14-day time period using pandas_ta.
    """
    df['rsi'] = ''
    rsi = list(pta.rsi(df['close'], time_period))
    df['rsi'] = rsi
    return df


def calculate_break_80(df):
    """
    This function checks all the points within the past year's RSI and returns the points that drop below 80.
    """
    df['break_80'] = 0
    for index in range(len(df) - 1):
        first = df['rsi'][index]
        second = df['rsi'][index + 1]
        if first >= 80 > second:
            df['break_80'][index + 1] = 1
    return df


def calculate_break_30(df):
    """
    This function checks all the points within the past year's RSI and returns the points across 30.
    """
    df['break_30'] = 0
    for index in range(len(df) - 1):
        first = df['rsi'][index]
        second = df['rsi'][index + 1]
        if first <= 30 < second:
            df['break_30'][index + 1] = 1
    return df


def calculate_down_trends(input_dataframe, res_dataframe):
    """
    This function uses break 80 points to identify all downtrends within the past year's data.
    It returns start and end dates, number of candles, price variation and It's percentage for each of downtrends.
    """
    # Fill start date
    for item in input_dataframe['break_80']:
        if item == 1:
            index = input_dataframe[input_dataframe['break_80'] == item].index.values
            res_dataframe['start_date'] = input_dataframe['time'][index]

    for date in res_dataframe['start_date']:
        res_dataframe_index = int(res_dataframe[res_dataframe['start_date'] == date].index.values)
        input_dataframe_index = int(input_dataframe[input_dataframe['time'] == date].index.values)
        rsi = input_dataframe['rsi'][input_dataframe_index]
        i = 1
        first = rsi
        second = input_dataframe['rsi'][input_dataframe_index + i]
        if first <= second:
            # The downtrend did not continue
            res_dataframe['end_date'][res_dataframe_index] = date
            res_dataframe['number_of_candles'][res_dataframe_index] = 0
            res_dataframe['price_change'][res_dataframe_index] = 0
            res_dataframe['percentage'][res_dataframe_index] = 0
        else:
            # The downtrend continued
            number_of_candles = 0
            while first > second:
                first = second
                i += 1
                number_of_candles += 1
                second = input_dataframe['rsi'][input_dataframe_index + i]
                index = int(input_dataframe[input_dataframe['rsi'] == first].index.values)
                res_dataframe['end_date'][res_dataframe_index] = input_dataframe['time'][index]
                res_dataframe['number_of_candles'][res_dataframe_index] = number_of_candles
                res_dataframe['price_change'][res_dataframe_index] = input_dataframe['close'][index] - input_dataframe[
                    'close'][input_dataframe_index]
                res_dataframe['percentage'][res_dataframe_index] = ((input_dataframe['close'][index] - input_dataframe[
                    "close"][input_dataframe_index]) / dataframe['close'][input_dataframe_index]) * 100
    return res_dataframe


def calculate_up_trends(input_dataframe, res_dataframe):
    """
    This function uses break 30 points to identify all uptrends within the past year's data.
    It returns start and end dates, number of candles, price variation and It's percentage for each of uptrends.
    """
    for item in input_dataframe['break_30']:
        if item == 1:
            index = input_dataframe[input_dataframe['break_30'] == item].index.values
            res_dataframe['start_date'] = input_dataframe['time'][index]

    for date in res_dataframe['start_date']:
        res_dataframe_index = int(res_dataframe[res_dataframe['start_date'] == date].index.values)
        input_dataframe_index = int(input_dataframe[input_dataframe['time'] == date].index.values)
        rsi = input_dataframe['rsi'][input_dataframe_index]
        i = 1
        first = rsi
        second = input_dataframe['rsi'][input_dataframe_index + i]
        if first >= second:
            # The uptrend did not continue
            res_dataframe['end_date'][res_dataframe_index] = date
            res_dataframe['number_of_candles'][res_dataframe_index] = 0
            res_dataframe['price_change'][res_dataframe_index] = 0
            res_dataframe['percentage'][res_dataframe_index] = 0
        else:
            # The uptrend continued
            number_of_candles = 0
            while first < second:
                first = second
                i += 1
                number_of_candles += 1
                second = input_dataframe['rsi'][input_dataframe_index + i]
                index = int(input_dataframe[input_dataframe['rsi'] == first].index.values)
                res_dataframe['end_date'][res_dataframe_index] = input_dataframe['time'][index]
                res_dataframe['number_of_candles'][res_dataframe_index] = number_of_candles
                res_dataframe['price_change'][res_dataframe_index] = input_dataframe['close'][index] - input_dataframe[
                    "close"][input_dataframe_index]
                res_dataframe['percentage'][res_dataframe_index] = ((input_dataframe['close'][index] - input_dataframe[
                    "close"][input_dataframe_index]) / input_dataframe['close'][input_dataframe_index]) * 100
    return res_dataframe


preprocessed_df = preprocess_dataframe(dataframe)
rsi_df = calculate_rsi(preprocessed_df, 14)
break_80_df = calculate_break_80(rsi_df)
break_30_df = calculate_break_30(rsi_df)
downtrends = calculate_down_trends(break_80_df, down_trends)
uptrends = calculate_up_trends(break_30_df, up_trends)

print(f'Uptrends : \n {downtrends}')
print(f'Downtrends: \n {uptrends}')
