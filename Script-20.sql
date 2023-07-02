
SELECT e.Event_ID, p.Name, e.Hall_number, t.Seat_number--Запрос с внешним соединением и проверкой на наличие NULL
From (Events e left join Tickets t on e.Event_ID = t.Event_ID) join Plays p on p.Play_ID = e.Play_ID
where t.visitor_ID is null

select avg(a.Salary) as aver_young --Простой запрос с условием и формулами в SELECT
from actors a
where a.Experience <= 5

Select (avg(a.Salary) - (select avg(a.Salary) from actors a where a.Experience <= 5)) as Salary_difference --Запрос с коррелированным подзапросом в SELECT
from Actors a

select (max(a.age)- min(a.age)) as age_difference --Простой запрос с условием и формулами в SELECT
from actors a
where a.Experience > 4

select v.Visitor_ID, v.Full_name, count(v.Visitor_ID) as ticket_quantity --Запрос с агрегированием и выражением JOIN, включающим не менее 2 таблиц
from Tickets t join Visitors v on v.Visitor_ID = t.Visitor_ID
group by v.Visitor_ID, v.Full_name

SELECT v.Visitor_ID, v.Full_name, sum(t.Price) as total_spendings --Запрос с агрегированием и выражением JOIN, включающим не менее 2 таблиц
FROM Tickets t join Visitors v on v.Visitor_ID = t.Visitor_ID
group by v.Visitor_ID, v.Full_name

SELECT top 1 v.Visitor_ID, v.Full_name, sum(t.Price) as total_spendings--Запрос с агрегированием и выражением JOIN, включающим не менее 2 таблиц
FROM Tickets t join Visitors v on v.Visitor_ID = t.Visitor_ID
group by v.Visitor_ID, v.Full_name
order by total_spendings desc

SELECT v.Visitor_ID, v.Full_name,
"discount, %"=case when sum(t.Price)>=1100 and sum(t.Price)<2000 then 5 when sum(t.Price)>=2000 then 10 else 0--Запрос с агрегированием и выражением JOIN, включающим не менее 2 таблиц
end
FROM Tickets t join Visitors v on v.Visitor_ID = t.Visitor_ID
group by v.Visitor_ID, v.Full_name

select h.Genre_ID, ROUND(100.0*count(Genre_ID)/(select count(Genre_ID) from (Tickets t join Events e on e.Event_ID = t.Event_ID) join Have h on e.Play_ID = h.Play_ID), 2) as 'Genre_popularity, %'
from (Tickets t join Events e on e.Event_ID = t.Event_ID) join Have h on e.Play_ID = h.Play_ID --Запрос с коррелированным подзапросом в SELECT
where t.Visitor_ID is not null
group by h.Genre_ID

SELECT sum(Price) as lost_money --Запрос с подзапросом в FROM
FROM (SELECT p.Name, e.Hall_number, t.Seat_number, t.Price
From (Events e left join Tickets t on e.Event_ID = t.Event_ID) join Plays p on p.Play_ID = e.Play_ID
where t.visitor_ID is null) as lost_money

Select sum(total_spendings) as total_revenue --Запрос с подзапросом в FROM
from(SELECT v.Visitor_ID, v.Full_name, sum(t.Price) as total_spendings
FROM Tickets t join Visitors v on v.Visitor_ID = t.Visitor_ID
group by v.Visitor_ID, v.Full_name) as total_revenue

SELECT v.Visitor_ID, v.Full_name, --Запрос с HAVING и агрегированием
"discount, %"=case when sum(t.Price)>=1100 and sum(t.Price)<2000 then 5 when sum(t.Price)>=2000 then 10 else 0
end
FROM Tickets t join Visitors v on v.Visitor_ID = t.Visitor_ID
group by v.Visitor_ID, v.Full_name
having case when sum(t.Price)>=1100 and sum(t.Price)<2000 then 5 when sum(t.Price)>=2000 then 10 else 0
end > 0

select * --Запрос с EXISTS
From Visitors v
where exists(select * from Tickets t where t.Visitor_ID = v.Visitor_ID)

SELECT [date], --Запрос, использующий оконную функцию LAG или LEAD для выполнения сравнения данных в разных периодах
sum(price)-lag(sum(price),1,0) over(order by [date]) revenue_difference
FROM Tickets t join Events e on e.Event_ID = t.Event_ID
group by [Date] 

select p.Play_ID, p.Name, avg(a.Age) as avg_age --Запрос с агрегированием и выражением JOIN, включающим не менее 3 таблиц/выражений
from (Plays p join Play_in pi2 on p.Play_ID = pi2.Play_ID) join Actors a on pi2.Actor_ID = a.Actor_ID 
Group by p.Play_ID, p.Name  

(select null as 'Director_ID', Actor_ID, Full_name, Age, Experience, Title, Salary from Actors a) --Запрос, использующий манипуляции с множествами
union
(select Director_ID, null, Full_name, Age, Experience, Title, Salary from Directors d)

Денис Кононов, [17.06.2023 21:42]
select Actor_ID, Full_name, Salary, (round((select avg(salary) from Actors a), 2)) as avg_salary --Запрос с коррелированным подзапросом в WHERE
from Actors a 
where salary > (select avg(salary) from Actors a)

select Visitor_ID, Full_name --Запрос с коррелированным подзапросом в WHERE
from Visitors v 
where Visitor_ID in (select Visitor_ID from Tickets t where price>=1200)

SELECT Actor_ID, Full_name, 0.1*sum(Salary) as bonus --Запрос с подзапросом в FROM, агрегированием, группировкой и сортировкой
from (select e.Event_ID, a.Actor_ID, a.Full_name, a.Salary 
from (Events e join Play_in pi2 on e.Play_ID = pi2.Play_ID) join Actors a on a.Actor_ID = pi2.Actor_ID 
where a.Title is not null) as title_actors
group by Actor_ID, Full_name 
order by sum(Salary) DESC

SELECT Event_ID, 0.1*sum(Salary) as bonus --Запрос с подзапросом в FROM, агрегированием, группировкой и сортировкой
from (select e.Event_ID, a.Actor_ID, a.Full_name, a.Salary 
from (Events e join Play_in pi2 on e.Play_ID = pi2.Play_ID) join Actors a on a.Actor_ID = pi2.Actor_ID 
where a.Title is not null) as title_actors
group by Event_ID 
order by sum(Salary) DESC

create trigger ticket_creation --триггер (создание билетов при добавлении мероприятия)
on Events
after INSERT 
as
begin
  declare @seats int = (select h.seat_quantity from inserted join Halls h on inserted.Hall_number = h.Hall_number )
  declare @seats1 int = (select h.seat_quantity from inserted join Halls h on inserted.Hall_number = h.Hall_number )
  declare @c_seat int = 1
  while @seats > 0
  begin
    if @c_seat between 1 and floor(@seats1-2*@seats1/3) 
      insert into Tickets (Price, Seat_number, Visitor_ID, Event_ID)
      select 1500, @c_seat, null, inserted.Event_ID from inserted
    else if @c_seat between floor(@seats1-2*@seats1/3)+1 and floor(@seats1-@seats1/3)
      insert into Tickets (Price, Seat_number, Visitor_ID, Event_ID)
      select 1200, @c_seat, null, inserted.Event_ID from inserted
    else
      insert into Tickets (Price, Seat_number, Visitor_ID, Event_ID)
      select 1000, @c_seat, null, inserted.Event_ID from inserted
    set @c_seat = @c_seat + 1
    set @seats = @seats - 1
  end  
end

create procedure ticket_purchase( --Процедура покупки билета
@event int,
@visitor INT,
@seat int
)
as
BEGIN 
  update Tickets 
  set Visitor_ID = @visitor
  where (Event_ID = @event) and (Seat_number = @seat)
END 


alter procedure title_update( --процедура (изменения звания в зависимости от достижений)
@actor_id int
)
as
BEGIN 
  declare @cnt int = 0
  if (select count(e.Event_ID) 
from Events e join Play_in pi2 on e.Play_ID = pi2.Play_ID
where pi2.Actor_ID = @actor_id) >= 0
    set @cnt = @cnt+1;   
   if (SELECT top 1 100*count(t.Ticket_ID)/h.seat_quantity as attendance_percentage
from ((Tickets t join Events e on t.Event_ID = e.Event_ID) join Play_in pi2 on e.Play_ID = pi2.Play_ID) join Halls h on e.Hall_number = h.Hall_number 
where (pi2.Actor_ID = @actor_id) and t.Visitor_ID is not null
group by e.Event_ID, h.seat_quantity  
order by attendance_percentage) >= 0
  set @cnt = @cnt + 1;  
  if (select COUNT(t.Visitor_ID) 
from (Tickets t join Events e on t.Event_ID = e.Event_ID) join Play_in pi2 on e.Play_ID = pi2.Play_ID
where (pi2.Actor_ID = @actor_id) and t.Visitor_ID is not null)>= 0
set @cnt = @cnt + 1;  
  if @cnt = 1
  update Actors 
  set title = 'copper'
  where Actor_ID = @actor_id
  ELSE if @cnt = 2
  update Actors 
  set title = 'silver'
  where Actor_ID = @actor_id
  else 
  update Actors 
  set title = 'gold'
  where Actor_ID = @actor_id
END 

create procedure benefits( --процедура (повышение зарплаты в зависимости от опыта)
@dir_id int
)
as 
begin
  update Directors 
  set Salary = Salary*1.2
  where Director_ID = @dir_id  and exists (select * from directors d where d.Experience >= 20)  
end

ALTER  procedure salary_raise(
@ev_id int
)
as 
BEGIN 
    Begin try 
        begin TRANSACTION 
        if 100 * (select COUNT(Price) 
from Tickets t
where (t.Event_ID = @ev_id) and (Price<1500))/(select COUNT(Price) 
from Tickets t
where (t.Event_ID = @ev_id) and (Price>=1500)) >50
        update Directors 
        set salary = 1.01 * salary
        where exists (select d.Director_ID 
from ((Events e join Plays p on e.Play_ID = p.Play_ID) join Stage s on p.Play_ID = s.Play_ID) join Directors d on s.Director_ID = d.Director_ID
where e.Event_ID = @ev_id)
        commit transaction
    end try
    Begin catch
        if (select COUNT(Price) 
from Tickets t
where (t.Event_ID = @ev_id) and (Price>=1500)) = 0
        rollback transaction;
        DECLARE @msg nvarchar(2048) = error_message()
    end catch
END
exec salary_raise  7

ALTER  procedure get_ticket( --процедура вывода билетов клиенту
@vis_id int
)
as
begin
    select t.Ticket_ID, p.Name, e.[Date], t.Seat_number, e.Hall_number 
    from (tickets t join events e on t.Event_ID = e.Event_ID) join Plays p on e.Play_ID = p.Play_ID
    where t.Visitor_ID = @vis_id
end

EXEC get_ticket 2

Create Function VisByTicket (@ticket_price int)
Returns int 
as 
begin 
	DECLARE @visitorCount int ; 
    Select @visitorCount = Count(DISTINCT visitor_ID)
    FROM Tickets 
    where Price  = @ticket_price;
   
   Return @visitorCount
end

DECLARE @price DECIMAL(10, 2) = 1000;
SELECT @price AS Price, dbo.VisByTicket(@price) AS Visitor_Count;

Create Function VisByTicket (@ticket_price int) --функция для подсчета сколько людей купило билет за определенную цену
Returns int 
as 
begin 
    DECLARE @visitorCount int ; 
    Select @visitorCount = Count(DISTINCT visitor_ID)
    FROM Tickets 
    where Price  = @ticket_price;
   Return @visitorCount
end

alter function SalaryByTitle (@tit nvarchar(50)) --функция для подсчета средней зарплаты по определенному званию
returns decimal(10, 2)
as
begin
  declare @s decimal(10, 2)
  select @s = avg(Salary)
  from actors a
  where a.title = @tit
  return @s
end

create view ActorsInPlays --представление для удобного просмотра актеров в спектаклях
as 
select p.Name, a.Full_name
from (Plays p join Play_in pi2 on p.Play_ID = pi2.Play_ID) join Actors a on pi2.Actor_ID = a.Actor_ID

create view TicketInformation -- представление для удобного просмотра информации о всех билетах
as
select t.Ticket_ID, p.Name, e.[Date], t.Seat_number, e.Hall_number 
  from (tickets t join events e on t.Event_ID = e.Event_ID) join Plays p on e.Play_ID = p.Play_ID
