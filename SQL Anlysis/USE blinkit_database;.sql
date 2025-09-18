create database finance;
use finance;

DROP table if exists staging_loans;
CREATE TABLE staging_loans (
    loan_id           VARCHAR(50),
    address_state       VARCHAR(50),
    application_type    VARCHAR(50),
    emp_length          VARCHAR(50),
    emp_title           VARCHAR(255),
    grade               VARCHAR(5),
    home_ownership      VARCHAR(50),
    issue_date          VARCHAR(50),
    last_credit_pull_date  VARCHAR(50),
    last_payment_date      VARCHAR(50),
    loan_status         VARCHAR(50),
    next_payment_date      VARCHAR(50),
    member_id          VARCHAR(50),
    purpose             VARCHAR(100),
    sub_grade           VARCHAR(10),
    term                VARCHAR(50),
    verification_status VARCHAR(50),
    annual_income       VARCHAR(50),
    dti                 VARCHAR(50),
    installment         VARCHAR(50),
    int_rate            VARCHAR(50),
    loan_amnt           VARCHAR(50),
    total_acc           VARCHAR(50),
    total_payment       VARCHAR(50)
);

-- 2) Load CSV directly into staging
LOAD DATA LOCAL INFILE 'D:\\Bank loan report\\financial_loan.csv'
INTO TABLE staging_loans
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from staging_loans
limit 10;

SET SQL_SAFE_UPDATES = 0;
UPDATE staging_loans
SET emp_title = 'Unknown'
WHERE emp_title IS NULL OR emp_title = '';
SET SQL_SAFE_UPDATES = 1; 

drop table if exists bank_report;
CREATE TABLE bank_report (
    loan_id             VARCHAR(50) PRIMARY KEY,
    address_state       VARCHAR(50),
    application_type    VARCHAR(50),
    emp_length          VARCHAR(100),
    emp_title           VARCHAR(255),
    grade               CHAR(1),
    home_ownership      VARCHAR(50),
    issue_date             DATE,
    last_credit_pull_date  DATE,
    last_payment_date      DATE,
    loan_status         VARCHAR(50),
    next_payment_date     DATE,
    member_id          VARCHAR(50),
    purpose             VARCHAR(100),
    sub_grade           VARCHAR(10),
    term                VARCHAR(20),
    verification_status VARCHAR(50),
    annual_income       DECIMAL(15,2),
    dti                 DECIMAL(5,2),
    installment         DECIMAL(10,2),
    int_rate            DECIMAL(5,2),
    loan_amnt           DECIMAL(12,2),
    total_acc           INT,
    total_payment       DECIMAL(15,2)
);

INSERT INTO bank_report (
    loan_id, address_state, application_type, emp_length, emp_title,
    grade, home_ownership, issue_date, last_credit_pull_date, last_payment_date,
    loan_status, next_payment_date, member_id, purpose, sub_grade,
    term, verification_status, annual_income, dti, installment,
    int_rate, loan_amnt, total_acc, total_payment
)
SELECT
	loan_id,
    address_state,
    application_type,
	emp_title,   
    emp_length,
    grade,
    home_ownership,
    STR_TO_DATE(issue_date, '%d-%m-%Y'),           
    STR_TO_DATE(last_credit_pull_date, '%d-%m-%Y'),
    STR_TO_DATE(last_payment_date, '%d-%m-%Y'),
    loan_status,
    STR_TO_DATE(next_payment_date, '%d-%m-%Y'),
    member_id,
    purpose,
    sub_grade,
    term,
    verification_status,
    CAST(annual_income AS DECIMAL(15,2)),
    CAST(dti AS DECIMAL(5,2)),
    CAST(installment AS DECIMAL(10,2)),
    CAST(REPLACE(int_rate, '%', '') AS DECIMAL(5,2)),  -- remove % sign
    CAST(loan_amnt AS DECIMAL(12,2)),
    CAST(total_acc AS UNSIGNED),
    CAST(total_payment AS DECIMAL(15,2))
FROM staging_loans;

select * from bank_report
limit 10;

-- KPI's calculations
-- Total Loan Applications
SELECT COUNT(loan_id) AS Total_Applications 
FROM bank_report;

-- MTD Loan Applications (loans issued in December of any year)
SELECT COUNT(loan_id) AS Total_Applications 
FROM bank_report
WHERE MONTH(issue_date) = 12;

-- Total Funded Amount (sum of all loan amounts disbursed)
SELECT SUM(loan_amnt) AS Total_Funded_Amount_crore
FROM bank_report;

-- MTD Total Funded Amount (loan amount disbursed in December)
SELECT SUM(loan_amnt) AS Total_Funded_Amount_crore 
FROM bank_report
WHERE MONTH(issue_date) = 12;

-- Total Amount Received (sum of all payments collected)
SELECT SUM(total_payment) AS Total_Amount_Collected 
FROM bank_report;

-- MTD Total Amount Received (payments collected from loans issued in December)
SELECT SUM(total_payment) AS Total_Amount_Collected 
FROM bank_report
WHERE MONTH(issue_date) = 12;

-- Average Interest Rate across all loans (in %)
SELECT AVG(int_rate) * 100 AS Avg_Int_Rate 
FROM bank_report;

-- MTD Average Interest Rate (for loans issued in December)
SELECT AVG(int_rate) * 100 AS MTD_Avg_Int_Rate 
FROM bank_report
WHERE MONTH(issue_date) = 12;

-- Average Debt-to-Income ratio (DTI in %)
SELECT AVG(dti) * 100 AS Avg_DTI 
FROM bank_report;

-- MTD Average DTI (for loans issued in December)
SELECT AVG(dti) * 100 AS MTD_Avg_DTI 
FROM bank_report
WHERE MONTH(issue_date) = 12;

-- GOOD LOAN
-- Good Loan Percentage (loans that are Fully Paid or Current as % of total)
SELECT
    (COUNT(CASE WHEN loan_status IN ('Fully Paid', 'Current') THEN loan_id END) * 100.0) / 
    COUNT(loan_id) AS Good_Loan_Percentage
FROM bank_report;

-- Good Loan Applications (count of loans that are Fully Paid or Current)
SELECT COUNT(loan_id) AS Good_Loan_Applications 
FROM bank_report
WHERE loan_status IN ('Fully Paid', 'Current');

-- Good Loan Funded Amount (sum of loan amount disbursed for Fully Paid or Current loans)
SELECT SUM(loan_amnt) AS Good_Loan_Funded_Amount 
FROM bank_report
WHERE loan_status IN ('Fully Paid', 'Current');

-- Good Loan Amount Received (sum of payments collected from Fully Paid or Current loans)
SELECT SUM(total_payment) AS Good_Loan_Amount_Received 
FROM bank_report
WHERE loan_status IN ('Fully Paid', 'Current');


-- BAD LOAN
-- Bad Loan Percentage (loans Charged Off as % of total)
SELECT
    (COUNT(CASE WHEN loan_status = 'Charged Off' THEN loan_id END) * 100.0) / 
    COUNT(loan_id) AS Bad_Loan_Percentage
FROM bank_report;

-- Bad Loan Applications (count of loans that are Charged Off)
SELECT COUNT(loan_id) AS Bad_Loan_Applications 
FROM bank_report
WHERE loan_status = 'Charged Off';

-- Bad Loan Funded Amount (sum of loan amount disbursed for Charged Off loans)
SELECT SUM(loan_amnt) AS Bad_Loan_Funded_Amount 
FROM bank_report
WHERE loan_status = 'Charged Off';

-- Bad Loan Amount Received (sum of payments collected from Charged Off loans)
SELECT SUM(total_payment) AS Bad_Loan_Amount_Received 
FROM bank_report
WHERE loan_status = 'Charged Off';

-- LOAN STATUS
-- Loan status summary: count of loans, total received, total funded, avg interest rate & avg DTI by loan status
SELECT
    loan_status,
    COUNT(loan_id) AS LoanCount,
    SUM(total_payment) AS Total_Amount_Received,
    SUM(loan_amnt) AS Total_Funded_Amount,
    AVG(int_rate * 100) AS Interest_Rate,
    AVG(dti * 100) AS DTI
FROM bank_report
GROUP BY loan_status;

-- Loan status summary for December (MTD): total received & total funded grouped by loan status
SELECT 
    loan_status, 
    SUM(total_payment) AS MTD_Total_Amount_Received, 
    SUM(loan_amnt) AS MTD_Total_Funded_Amount 
FROM bank_report
WHERE MONTH(issue_date) = 12 
GROUP BY loan_status;



-- BANK LOAN REPORT | OVERVIEW
-- Monthly loan summary: applications, funded amount, and amount received grouped by month
SELECT 
    MONTH(issue_date) AS Month_Number, 
    MONTHNAME(issue_date) AS Month_Name, 
    COUNT(loan_id) AS Total_Loan_Applications,
    SUM(loan_amnt) AS Total_Funded_Amount,
    SUM(total_payment) AS Total_Amount_Received
FROM bank_report
GROUP BY MONTH(issue_date), MONTHNAME(issue_date)
ORDER BY Month_Number;

-- Loan summary by state: applications, funded amount, and amount received
SELECT 
    address_state AS State, 
    COUNT(loan_id) AS Total_Loan_Applications,
    SUM(loan_amnt) AS Total_Funded_Amount,
    SUM(total_payment) AS Total_Amount_Received
FROM bank_report
GROUP BY address_state
ORDER BY address_state;

-- Loan summary by term (e.g., 36 months vs 60 months)
SELECT 
    term AS Term, 
    COUNT(loan_id) AS Total_Loan_Applications,
    SUM(loan_amnt) AS Total_Funded_Amount,
    SUM(total_payment) AS Total_Amount_Received
FROM bank_report
GROUP BY term
ORDER BY term;

-- Loan summary by employee length (borrower’s years of employment)
SELECT 
    emp_length AS Employee_Length, 
    COUNT(loan_id) AS Total_Loan_Applications,
    SUM(loan_amnt) AS Total_Funded_Amount,
    SUM(total_payment) AS Total_Amount_Received
FROM bank_report
GROUP BY emp_length
ORDER BY emp_length;

-- Loan summary by purpose (e.g., car, credit card, house improvement)
SELECT 
    purpose AS Purpose, 
    COUNT(loan_id) AS Total_Loan_Applications,
    SUM(loan_amnt) AS Total_Funded_Amount,
    SUM(total_payment) AS Total_Amount_Received
FROM bank_report
GROUP BY purpose
ORDER BY purpose;

-- Loan summary by home ownership (Rent, Mortgage, Own)
SELECT 
    home_ownership AS Home_Ownership, 
    COUNT(loan_id) AS Total_Loan_Applications,
    SUM(loan_amnt) AS Total_Funded_Amount,
    SUM(total_payment) AS Total_Amount_Received
FROM bank_report
GROUP BY home_ownership
ORDER BY home_ownership;





