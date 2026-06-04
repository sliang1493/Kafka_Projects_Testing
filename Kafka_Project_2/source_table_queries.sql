drop table employees;
drop table emp_audit;

drop function insert_to_audit_log();
drop trigger trg_on_employees_change on employees;

truncate table employees;
truncate table emp_audit;


-- create source table and audit or CDC table
create table employees (
	emp_id SERIAL,
	first_name VARCHAR(100),
	last_name VARCHAR(100),
	dob DATE,
	city VARCHAR(100),
	salary INT
);

create table emp_audit (
	audit_id SERIAL primary key,
	emp_id INT,
	first_name VARCHAR(100),
	last_name VARCHAR(100),
	dob DATE,
	city VARCHAR(100),
	salary INT,
	crud_action VARCHAR(100)
);


-- function to handle CRUD changes to employees
create function insert_to_audit_log()
returns trigger as $$
begin
	IF TG_OP = 'INSERT' THEN
		INSERT INTO emp_audit (emp_id, first_name, last_name, dob, city, salary, crud_action)
		VALUES (new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary, 'INSERT');
	ELSEIF TG_OP = 'UPDATE' THEN
		INSERT INTO emp_audit (emp_id, first_name, last_name, dob, city, salary, crud_action)
		VALUES (new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary, 'UPDATE');
	ELSEIF TG_OP = 'DELETE' THEN
		INSERT INTO emp_audit (emp_id, first_name, last_name, dob, city, salary, crud_action)
		VALUES (old.emp_id, old.first_name, old.last_name, old.dob, old.city, old.salary, 'DELETE');
	END IF;
	RETURN NULL;
end;
$$ language plpgsql;


-- multi event trigger on CRUD changes to employees
create trigger trg_on_employees_change
after insert or update or delete on employees
for each row
execute function insert_to_audit_log();

select * from employees;
select * from emp_audit;


insert into employees (first_name, last_name, dob, city, salary)
values
('John', 'Doe', '1990-01-22', 'Chicago', 250000),
('Bill', 'Smith', '1995-10-13', 'Boston', 120000),
('Bruce', 'Wayne', '1979-05-01', 'Gotham', 10000000),
('Peter', 'Parker', '2002-01-01', 'Queens', 80000);

update employees set first_name = 'Indiana', last_name = 'Jones'
where emp_id = 2;

insert into employees (first_name, last_name, dob, city, salary)
values
('Clark', 'Kent', '1980-07-14', 'Metropolis', 70000);

delete from employees where emp_id = 1;
