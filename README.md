# Spark AIS Ships

This project processes AIS (Automatic Identification System) data, which is sourced from [Kystverket](https://www.kystverket.no/en/navigation-and-monitoring/ais/access-to-ais-data/), with their permission, in accordance with their license terms. The project leverages Apache Spark to load, clean, analyze, and visualize ship data, ultimately producing a map with ship locations.

## Table of Contents

- [Spark AIS Ships](#spark-ais-ships)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Requirements](#requirements)
    - [Step-by-Step Installation](#step-by-step-installation)
  - [Usage](#usage)
  - [File Structure](#file-structure)
  - [Modules](#modules)

## Installation

To run this project, you need to install the required dependencies and set up Apache Spark on your local machine.

### Requirements

Detailed requirements for specific versions of Apache Spark can be found in the official documentation: [Apache Spark Documentation](https://spark.apache.org/documentation.html). The project uses verions as below:

- Python 3.11.9
- Apache Spark 3.4.4
- JDK (Java Development Kit) 11
- Required Python libraries:
  - `pyspark` 3.4.4
  - `haversine` 2.9.0
  - `plotly` 6.0.1

### Step-by-Step Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/szymsta/spark_ais_ships.git
   cd spark_ais_ships
   ```

2. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Make sure that Apache Spark and Java are correctly installed. You can verify this by running the following commands:
   ```bash
   spark-shell --version
   java --version
   ```

## Usage

To run the pipeline, execute the **`main_app.py`** script, which initializes a Spark session, loads data, cleans it, analyzes it, and visualizes the results on a map. You can use any combination of the available functions in the modules for data analysis and visualization, depending on your needs. This flexibility allows for customized workflows and tailored insights from the AIS data, including filtering, calculating distances, and visualizing ships on a map.

1. Ensure your dataset is available and accessible by the script.

2. Run the main script:
   ```bash
   python main_app.py
   ```
3. This will:

   - Load and clean the AIS data using modules in the pipeline.

   - Analyze dynamic ship data, including distance and speed calculations.

   - Visualize ship locations on an interactive map.

## File Structure

Here’s an overview of the project directory:
```bash
/spark-ais-ships
  ├── main_app.py                   # Main script to run the pipeline
  ├── load_data/                    # Contains the LoadData module
  │   └── data_loader.py            # Module for loading AIS data
  ├── clean_data/                   # Contains the CleanData module
  │   └── data_cleaner.py           # Module for cleaning the loaded data
  ├── analyse_data/                 # Contains the AnalyzeData module
  │   └── data_analyzer.py          # Module for analyzing data
  ├── search_data/                  # Contains the SearchData module
  │   └── data_searcher.py          # Module for searching specific AIS data
  ├── visualize_data/               # Contains the VisualizeData module
  │   └── data_visualization.py     # Module for visualizing AIS data
  ├── config.py                     # Configuration file
  ├── requirements.txt              # Python dependencies
  ├── README.md                     # Project documentation
  └── mid_for_mmsi.csv              # Contains Maritime Identification Digits (MID) number for ship identification
  ```
  ## Modules

1. LoadData: The module is responsible for loading raw AIS data from a file and merging it with the MID numbers for each country into a single DataFrame.

2. CleanData: The module is responsible for cleaning the DataFrame.

3. AnalyzeData: The module is responsible for analyzing the data to calculate distances, speeds, and dynamic ship information.

4. SearchData: The module provides functionality for searching ships by MMSI or geographical location.

5. VisualizeData: The module is responsible for visualizing the data on a map.