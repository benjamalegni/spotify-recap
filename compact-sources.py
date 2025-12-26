import json
from pathlib import Path

import pandas as pd


def load_streaming_history(data_dir: Path) -> pd.DataFrame:
    """
    Load all Spotify Extended Streaming History JSON files in data_dir
    and return a single DataFrame.
    """
    json_paths = sorted(data_dir.glob("*.json"))
    if not json_paths:
        raise FileNotFoundError(f"No JSON found in {data_dir}")

    frames = []
    for path in json_paths:
        # Each file is already a JSON with a list of reproductions
        df = pd.read_json(path)
        df["source_file"] = path.name  # useful for traceability
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    # Folder where the JSON files are (adjust if the location changes)
    data_dir = Path(__file__).parent / "data" / "Spotify Extended Streaming History"
    output_csv = data_dir / "streaming_history_all.csv"
    streaming_df = load_streaming_history(data_dir)
    streaming_df.to_csv(output_csv, index=False)

    print(f"Data combined and saved to: {output_csv}")
    print(f"Total records: {len(streaming_df):,}")
