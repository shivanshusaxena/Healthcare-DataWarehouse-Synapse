CREATE DATABASE HealthcareDB
use HealthcareDB

--CREATE MASTER KEY ENCRYPTION by PASSWORD = 'Shiv@Azure#2024!';

CREATE EXTERNAL DATA SOURCE HealthcareADLS3
WITH(
    LOCATION = 'https://shivdevedls.dfs.core.windows.net/healthcare-project'
)

CREATE EXTERNAL FILE FORMAT CSVFormat
WITH(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2,
        USE_TYPE_DEFAULT = TRUE
        )
)


SELECT TOP 5 *
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS healthcare_data;




CREATE SCHEMA healthcare;

CREATE EXTERNAL TABLE healthcare.dim_patients
WITH (
    LOCATION = 'dim_patients/',
    DATA_SOURCE = HealthcareADLS3,
    FILE_FORMAT = CSVFormat
)
AS
SELECT DISTINCT
    Name,
    TRY_CAST(Age AS INT) AS Age,
    Gender,
    Blood_Type,
    Insurance_Provider
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name';



SELECT TOP 5 * FROM healthcare.dim_patients;



CREATE EXTERNAL TABLE healthcare.dim_hospitals
WITH (
    LOCATION = 'dim_hospitals/',
    DATA_SOURCE = HealthcareADLS3,
    FILE_FORMAT = CSVFormat
)
AS
SELECT DISTINCT
    Hospital
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name';


select top 5* from healthcare.dim_hospitals


CREATE EXTERNAL TABLE healthcare.dim_conditions
WITH (
    LOCATION = 'dim_conditions/',
    DATA_SOURCE = HealthcareADLS3,
    FILE_FORMAT = CSVFormat
)
AS
SELECT DISTINCT
    Medical_Condition,
    Medication,
    Admission_Type
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name';


CREATE EXTERNAL TABLE healthcare.fact_admissions
WITH (
    LOCATION = 'fact_admissions/',
    DATA_SOURCE = HealthcareADLS3,
    FILE_FORMAT = CSVFormat
)
AS
SELECT
    Name,
    Hospital,
    Medical_Condition,
    Doctor,
    Date_of_Admission,
    Discharge_Date,
    TRY_CAST(Billing_Amount AS FLOAT) AS Billing_Amount,
    TRY_CAST(Room_Number AS INT) AS Room_Number,
    Admission_Type,
    Test_Results
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name';



SELECT 
    Medical_Condition,
    COUNT(*) AS Total_Patients,
    ROUND(AVG(TRY_CAST(Billing_Amount AS FLOAT)), 2) AS Avg_Billing,
    ROUND(SUM(TRY_CAST(Billing_Amount AS FLOAT)), 2) AS Total_Billing
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name'
GROUP BY Medical_Condition
ORDER BY Avg_Billing DESC;







SELECT 
    Hospital,
    COUNT(*) AS Total_Patients,
    ROUND(AVG(TRY_CAST(Billing_Amount AS FLOAT)), 2) AS Avg_Billing,
    ROUND(SUM(TRY_CAST(Billing_Amount AS FLOAT)), 2) AS Total_Billing
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name'
GROUP BY Hospital
ORDER BY Total_Patients DESC;






SELECT 
    Admission_Type,
    AVG(DATEDIFF(day, Date_of_Admission, Discharge_Date)) AS Avg_Stay_Days,
    ROUND(AVG(TRY_CAST(Billing_Amount AS FLOAT)), 2) AS Avg_Billing,
    ROUND(SUM(TRY_CAST(Billing_Amount AS FLOAT)), 2) AS Total_Billing
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name'
GROUP BY Admission_Type
ORDER BY Avg_Stay_Days DESC;







SELECT 
    CASE 
        WHEN TRY_CAST(Age AS INT) BETWEEN 0 AND 18 THEN '0-18'
        WHEN TRY_CAST(Age AS INT) BETWEEN 19 AND 35 THEN '19-35'
        WHEN TRY_CAST(Age AS INT) BETWEEN 36 AND 50 THEN '36-50'
        WHEN TRY_CAST(Age AS INT) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+' 
    END AS Age_Group,
    COUNT(*) AS Total_Patients
FROM OPENROWSET(
    BULK 'https://shivdevedls.dfs.core.windows.net/healthcare-project/healthcare_dataset.csv',
    FORMAT = 'CSV',
    HEADER_ROW = TRUE
) WITH (
    Name VARCHAR(100),
    Age VARCHAR(10),
    Gender VARCHAR(10),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(100),
    Date_of_Admission VARCHAR(20),
    Doctor VARCHAR(100),
    Hospital VARCHAR(100),
    Insurance_Provider VARCHAR(100),
    Billing_Amount VARCHAR(20),
    Room_Number VARCHAR(20),
    Admission_Type VARCHAR(50),
    Discharge_Date VARCHAR(20),
    Medication VARCHAR(100),
    Test_Results VARCHAR(50)
) AS src
WHERE Name <> 'Name'
GROUP BY 
    CASE 
        WHEN TRY_CAST(Age AS INT) BETWEEN 0 AND 18 THEN '0-18'
        WHEN TRY_CAST(Age AS INT) BETWEEN 19 AND 35 THEN '19-35'
        WHEN TRY_CAST(Age AS INT) BETWEEN 36 AND 50 THEN '36-50'
        WHEN TRY_CAST(Age AS INT) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+' 
    END
ORDER BY Total_Patients DESC;
