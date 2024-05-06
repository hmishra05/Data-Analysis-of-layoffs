
------------------------------------------DATA CLEANING OF LAYOFF DATASET----------------------------------------------------------

--Removing Duplicates
--Standardize the data
-- Null Values or Blank Values
-- Removing unecessary columns

use layoff
select * from [dbo].[layoffs$]

--Creating a replica of the table for cleaning
select * into layoffs
from [dbo].[layoffs$]
where 1=0;

--inserting data
insert layoffs
select * from [dbo].[layoffs$]

--Selecting all the data from the new table
select * from layoffs


--Removing Duplicates
select *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
total_laid_off, date, stage, country, funds_raised_millions order by funds_raised_millions) as row_num
from layoffs;

--Selecting duplicate values 
with dups_select as (
select *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
total_laid_off, date, stage, country, funds_raised_millions order by funds_raised_millions) as row_num
from layoffs
)
select * from dups_select 
where row_num > 1;

--Checking which particular entries are duplicate
select * from layoffs
where company = 'Casper';
select * from layoffs
where company = 'Cazoo';
select * from layoffs
where company = 'Hibob';
select * from layoffs
where company = 'Wildlife Studios';
select * from layoffs
where company = 'Yahoo';

--Deleting the duplicate values 

with dups_select as (
select *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
total_laid_off, date, stage, country, funds_raised_millions order by funds_raised_millions) as row_num
from layoffs
)
delete from dups_select 
where row_num > 1;

--Standardizing the data (Scrutinizing the data using distinct clause)

select distinct(company)
from layoffs
order by 1; --Trailing whitespaces identified 
--Trimming the whitespaces
select TRIM(company)
from layoffs

select distinct(location)
from layoffs 
order by 1;

select distinct(industry) 
from layoffs
order by 1; --Multiple Entries for the same industry (Crypto) found
--FIXING THE ERROR FOR CRYPTO INDUSTRY
select industry 
from layoffs
where industry like '%crypto%'

select distinct(total_laid_off)
from layoffs
order by 1;

select distinct(percentage_laid_off) 
from layoffs
order by 1;

select distinct(stage)
from layoffs

select distinct(location), country
from layoffs
order by 1; --Two different entries found for the country United States 
--FIXING THE ERROR
select distinct(country) 
from layoffs
where country like '%United%'
order by country asc;

select distinct(funds_raised_millions)
from layoffs
order by 1;

--Updating the cleaned data for company 

update layoffs
set company = TRIM(company);

--Updating the enteries for industry

update layoffs
set industry = 'Crypto'
where industry like '%crypto%'; 

update layoffs
set industry = TRIM(industry)

--Updating the entries for the country column

update layoffs
set country = 'United States'
where country like '%United States.%'

--Converting Date - Time stamp to just date 
select date, convert(date, date) as date_only
from layoffs;

--Adding a new date column to the table to accomodate the date into it.
alter table layoffs
add layoff_date date;

--updating the date extracted from the datetime stamp to the new column
update layoffs
set layoff_date = convert(date, date);

--Deleting the old_date data with timestamp from the dataset
alter table layoffs
drop column date;

--Handling missing values
select * from layoffs
where total_laid_off is null;

--Invoking industries that represent NULL values and updating the missing values upon matching them with corresponding rows
select * from layoffs t1
join layoffs t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry = 'Null')
and t2.industry is not null; --(Some companies are found to be null and the corresponding table has the values)

--Updating the industry entry for the missing values

UPDATE t1
SET t1.industry = t2.industry
FROM layoffs t1
JOIN layoffs t2 ON t1.company = t2.company AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = 'Null'AND t2.industry IS NOT NULL);

--Handling missing values for the total_laid_off and percentage_laid_off column
--There are several columns with null values in both column collectively

select * 
from layoffs
where try_convert (float, percentage_laid_off) is null 
and percentage_laid_off is not null
and total_laid_off is null;

--------------------------------EXPLORATORY DATA ANALYSIS OF THE LAYOFFS DATASET--------------------------------------------------------

-- Total employees each company had laid off globally

select company, sum(total_laid_off) as total_laid_off_sum
from layoffs
group by company
order by total_laid_off_sum desc;

--Total employees laid off from each country

select country, sum(total_laid_off) as total_laid_off_sum
from layoffs
group by country
order by total_laid_off_sum desc;

--Years when the highest number of employees were laid off

select YEAR(layoff_date) as year_laid_off, sum(total_laid_off) as sum_laid_off_per_year
from layoffs
group by YEAR(layoff_date)
order by sum_laid_off_per_year desc;

--Which particular stage laid off their employees

select stage, sum(total_laid_off) as total_stage_laidoff
from layoffs
group by stage
order by 2 desc;

--How many offices of each company laid off their employees

select company, count(*) as total_count
from layoffs
group by company
order by 2 desc;

--Aggregating total layoffs for Year/Month 

select format(layoff_date, 'yyyy-MM') as lay_off_date, sum(total_laid_off) as sum_laid_off
from layoffs
where format(layoff_date, 'yyyy-MM') is not null
group by format(layoff_date, 'yyyy-MM');

--Rolling total by YYYY/ MM (The unbounded preceeding and current row is used to override the 
--restrictions that may cause error in the row)

WITH cte AS (
    SELECT FORMAT(layoff_date, 'yyyy-MM') AS lay_off_date, SUM(total_laid_off) AS sum_laid_off
    FROM layoffs
    WHERE FORMAT(layoff_date, 'yyyy-MM') IS NOT NULL
    GROUP BY FORMAT(layoff_date, 'yyyy-MM')
)
SELECT lay_off_date AS month_layoff, sum_laid_off, 
       SUM(sum_laid_off) OVER (ORDER BY lay_off_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_total
FROM cte;

--Which company laid off employees in the progressing years

select company, year(layoff_date) as year_laid_off, sum(total_laid_off) as total_laidoff
from layoffs
group by company, year(layoff_date)
order by company asc;

with year_laid_off as (
select company, year(layoff_date) as year_laid_off, sum(total_laid_off) as total_laidoff
from layoffs
group by company, year(layoff_date)
)
select *, dense_rank() over (partition by year_laid_off order by total_laidoff desc) as rank_laid_off
from year_laid_off
where year_laid_off is not null;








