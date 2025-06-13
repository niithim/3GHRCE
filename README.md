# CMS Data Processing Tool

This tool processes and stores CMS (Centers for Medicare & Medicaid Services) datasets, including skilled nursing facility (SNF) data, into both CSV files and MySQL database.

## Features

- Downloads data from multiple CMS datasets
- Processes various types of SNF (Skilled Nursing Facility) data:
  - SNF All Owners
  - SNF Enrollments
  - SNF Change of Ownership
  - SNF Entity Performance
  - SNF Cost Report
  - SNF Provider Info
  - State Average Data
- Saves data in CSV format
- Stores data in MySQL database
- Handles asynchronous data processing
- Includes comprehensive logging

## Prerequisites

- Python 3.8 or higher
- MySQL Server
- Git

## Installation

1. Clone the repository:
```bash
git clone https://github.com/niithim/3GHRCE.git
cd 3GHRCE
```

2. Install required Python packages:
```bash
pip install -r requirements.txt
```

3. Install Playwright browsers:
```bash
playwright install
```

4. Set up MySQL:
- Install MySQL Server
- Create a database named `cms_data`
- Default credentials (can be modified in the code):
  - Username: root
  - Password: Nithin@123
  - Host: localhost
  - Port: 3306

## Usage

Run the main script:
```bash
python main.py
```

The script will:
1. Download data from CMS datasets
2. Save CSV files in the `csv` folder
3. Attempt to store data in MySQL database

## Project Structure

```
code_repo/
├── main.py              # Main processing script
├── requirements.txt     # Python dependencies
├── csv/                # Directory for CSV files
│   ├── provider.csv
│   ├── state_average.csv
│   └── other_dataset_files.csv
└── README.md           # This file
```

## Dependencies

- requests==2.31.0
- playwright==1.42.0
- pandas==2.2.1
- httpx==0.27.0
- mysql-connector-python==8.3.0
- asyncio==3.4.3
- typing-extensions==4.10.0

## Error Handling

The script includes comprehensive error handling and logging:
- Logs all operations with timestamps
- Handles API request failures
- Manages database connection issues
- Provides detailed error messages

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

Your Name - [@your_twitter](https://twitter.com/your_twitter)
Project Link: [https://github.com/niithim/3GHRCE](https://github.com/niithim/3GHRCE)

---

##  Datasets Handled

| # | Dataset Name                         | Method     | Source Slug / ID |
|---|--------------------------------------|------------|------------------|
| 1 | SNF All Owners                       | API        | `skilled-nursing-facility-all-owners` |
| 2 | SNF Enrollments                      | API        | `skilled-nursing-facility-enrollments` |
| 3 | SNF Change of Ownership              | API        | `skilled-nursing-facility-change-of-ownership` |
| 4 | SNF Entity Performance               | API        | `nursing-home-affiliated-entity-performance-measures` |
| 5 | SNF Cost Report                      | API        | `skilled-nursing-facility-cost-report` |
| 6 | SNF Provider Info                    | CSV Direct | `4pq5-n9py` |
| 7 | SNF State Average Performance        | CSV Direct | `xcdc-v8bm` |

---

## 🔧 Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/niithim/3GHRCE.git
cd 3GHRCE
2. Install Python Dependencies
   pip install -r requirements.txt
3. Install Playwright Browsers (for API UUID extraction)
   playwright install
4. Setup MySQL
Ensure MySQL is running locally and accessible with:

Username: root

Password: Nithin@123

Database: cms_data (auto-created)
Run the Project
 python test.py

