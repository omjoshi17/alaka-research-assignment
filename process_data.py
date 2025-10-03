import pandas as pd
import os
#importing the libs

#generalised func for all files
def generate_5min_candles(input_path, output_dir):
    
    try:
        print(f"Processing: {os.path.basename(input_path)}...")
        
        # loading the data
        df = pd.read_parquet(input_path)
        # prepare/filter the data
        time_column = 'date' 
        df[time_column] = pd.to_datetime(df[time_column])
        df_jan10 = df[df[time_column].dt.date == pd.to_datetime('2024-01-10').date()].copy()

        if df_jan10.empty:
            print(f"-> No data for Jan 10, 2024 in this file. Skipping.")
            return

        df_jan10.set_index(time_column, inplace=True)

        # resample to 5-minute candles
        candles_5min = df_jan10.resample('5min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        candles_5min.dropna(inplace=True)
        print("-> Resampled data to 5-minute candles.")

        #calculates and displays fibonacci pivot points
        day_high = df_jan10['high'].max()
        day_low = df_jan10['low'].min()
        day_close = df_jan10['close'].iloc[-1]
        
        P = (day_high + day_low + day_close) / 3
        R1 = P + 0.382 * (day_high - day_low)
        R2 = P + 0.618 * (day_high - day_low)
        R3 = P + (day_high - day_low)
        S1 = P - 0.382 * (day_high - day_low)
        S2 = P - 0.618 * (day_high - day_low)
        S3 = P - (day_high - day_low)

        base_name = os.path.basename(input_path).replace('.parquet.gz', '')
        
        print(f"\n--- Fibonacci Pivot Points for {base_name} on 2024-01-10 ---")
        print(f"Pivot Point (P): {P:.2f}")
        print("-" * 20)
        print(f"Resistance 1 (R1): {R1:.2f}, Resistance 2 (R2): {R2:.2f}, Resistance 3 (R3): {R3:.2f}")
        print(f"Support 1 (S1): {S1:.2f},    Support 2 (S2): {S2:.2f},    Support 3 (S3): {S3:.2f}\n")

        # save the 5-minute candle results
        new_filename = base_name + '.csv'
        output_file_path = os.path.join(output_dir, new_filename)
        candles_5min.to_csv(output_file_path)
        print(f"-> Successfully saved candle data to {new_filename}")

    except Exception as e:
        print(f"-> error processing {os.path.basename(input_path)}: {e}")



if __name__ == "__main__":
    print("--- Starting script to process all files ---")
    
    INPUT_DATA_DIR = 'data/candles/BANKNIFTY/'
    OUTPUT_CANDLE_DIR = '5min_candles/BANKNIFTY/2024-01-10/'
    os.makedirs(OUTPUT_CANDLE_DIR, exist_ok=True)

    for filename in os.listdir(INPUT_DATA_DIR):
        if filename.endswith(".parquet.gz"):
            input_file = os.path.join(INPUT_DATA_DIR, filename)
            generate_5min_candles(input_file, OUTPUT_CANDLE_DIR)
            
    print("--- All files processed---")