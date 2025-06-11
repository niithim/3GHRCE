 CMS Skilled Nursing Facility Data Pipeline
This project automates the extraction, transformation, and loading (ETL) of multiple Skilled Nursing Facility (SNF) datasets from the  
   [CMS (Centers for Medicare & Medicaid Services)] (https://data.cms.gov/) portal. 
It fetches data from both **API-based** and **CSV-based** datasets, processes them into structured format using Pandas, and stores them into a local MySQL database.

Project Structure

##  Features

-  Automated UUID extraction using Playwright from Swagger API docs
-  Fetch paginated dataset records via CMS JSON APIs
-  Download and parse CMS-hosted CSV datasets
-  Normalize and rename long columns for MySQL compatibility
-  Create MySQL tables dynamically with correct schema
-  Insert data into MySQL with automatic type conversion
-  Save local CSV backup for each dataset
-  Logging at every step for traceability and debugging

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

