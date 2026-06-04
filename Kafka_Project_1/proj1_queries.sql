create table if not exists Department_Employee (
	emp_id SERIAL primary key,
	department VARCHAR(50),
	department_division VARCHAR(50),
	position_title VARCHAR(200),
	hire_date DATE,
	salary INT
);

create table if not exists Department_Totals(
	department VARCHAR(50) UNIQUE,
	total_salary INT
);

select * from Department_Employee order by department_division;
select * from Department_Totals;

drop table if exists Department_Employee;
drop table if exists Department_Totals;

truncate table Department_Employee;
truncate table Department_Totals;
