select * from artist
select * from canvas_size
select * from image_link
select * from museum
select * from museum_hours
select * from product_size
select * from work
select * from subject

--Updating 'UK' to 'United Kingdom' in the museum, as we have two references for the same."  

  update museum set country='United Kingdom' where country='UK' 

--Deleting duplicate records from work, product_size, subject and image_link table



  with cte as (
  select*,row_number()over(partition by work_id order by  work_id asc) as rn from work
  ) delete from cte where rn >1
  
  
  
  with cte as (
  select*,row_number()over(partition by work_id ,size_id order by work_id ) as rn from product_size
  ) delete from cte where rn >1
  
  with cte as (
  select*,row_number()over(partition by work_id ,subject order by work_id ) as rn from [subject]
  ) delete from cte where rn >1
  
  with cte as (
  select*,row_number()over(partition by work_id  order by work_id ) as rn from image_link
  ) delete  from cte where rn >1
  
  with cte as (
  	select *, row_number()over(partition by museum_id,[day] order by museum_id ) as rn from museum_hours 
  	) delete from cte where rn >1
  
  
  
--1.Fetch all the paintings which are not displayed on any museums?

select * from work where museum_id is null;

--2. Are there museums without any paintings?

select * from museum where museum_id not in (select distinct museum_id from work)

--3. How many paintings have an asking price of more than their regular price

select count(work_id) as no_of_painitngs from (
  select work_id from  product_size where sale_price>regular_price
  ) as q


--4. Identify the paintings whose asking price is less than 50% of its regular price
select * 
	from product_size
	where sale_price < (regular_price*0.5);


--5. Which canva size costs the most?

select 
    label,sale_price 
from (
      select c.label,sale_price,
         dense_rank()over(order by sale_price desc) as rn  from canvas_size as c
		 inner join product_size as p on c.size_id=p.size_id
		 ) as q
where rn=1


--6) Identify the museums with invalid city information in the given dataset

	select * from museum 
	where city like '[0-9]%'

--7) Fetch the top 10 most famous painting subject

select * from (
select s.subject,count(1) as no_of_paintings,rank()over(order by count(1) desc) as rn from [subject] as s
				inner join work as w on s.work_id=w.work_id
				group by s.subject
				) as q where rn <=10


--8. Identify the museums which are open on both Sunday and Monday. Display
--museum name, city.

with cte as (
        select 
	    museum_id,
		day,
		lead(day,1) over(partition by museum_id order by museum_id asc) as lead1 
	    from museum_hours 
        where [day] in ('sunday','monday')),

 cte2 as (
        select *,
	    case when [day]='sunday' and lead1='monday' then museum_id 
        else '0' end as m_id from cte),
 cte3 as (
        select * 
		from museum_hours 
		where museum_id in (select m_id from cte2) and [day] in ('sunday','monday')) 

select * from museum where museum_id in ( select museum_id from cte3) order by name asc


--9) How many museums are open every single day?

with cte as (
     select museum_id from museum_hours 
     group by museum_id
     having count(*) =7
)select count(museum_id) from cte


--10) Which are the top 5 most popular museum? (Popularity is defined based on most no of paintings in a museum)


select m.name as museum, m.city,m.country,x.no_of_painintgs
	from (	select m.museum_id, count(1) as no_of_painintgs
			, rank() over(order by count(1) desc) as rnk
			from work w
			join museum m on m.museum_id=w.museum_id
			group by m.museum_id) x
	join museum m on m.museum_id=x.museum_id
	where x.rnk<=5;

--11. Who are the top 5 most popular artist? (Popularity is defined based on most no of
--paintings done by an artist)
select a.full_name ,a.nationality,a.style,a.birth,a.death from

(select w.artist_id,DENSE_RANK()over(order by count(w.work_id) desc) as rn  from artist as a
				inner join  work as w on a.artist_id=w.artist_id
				group by w.artist_id
				) as w  inner join artist as a 
				  on w.artist_id=a.artist_id
				  where w.rn<=5


--12) Display the 3 least popular canva sizes
	select label,ranking,no_of_paintings
	from (
		select cs.size_id,cs.label,count(1) as no_of_paintings
		, dense_rank() over(order by count(1) asc ) as ranking
		from work w
		join product_size ps on ps.work_id=w.work_id
		join canvas_size cs on cs.size_id = ps.size_id
		group by cs.size_id,cs.label) x
	where x.ranking<=3;

--13) Which museum has the most no of most popular painting style?
with cte as (

      select style from (
      select style,count(*) as no_of_style,dense_rank()over(order by count(*) desc ) as rn from work 
      where style is not null
      group by style
      ) as w where rn =1),
 
cte2 as  (
       select m.museum_id,m.name as museum_name,m.city,w.name as painting_name,w.style from museum m 
            inner join work as w on m.museum_id=w.museum_id
			where w.style in(select style from cte)),

cte3 as (select 
        museum_id,museum_name,count(style) as no_of_paintings,
		DENSE_RANK()over( order by count(style) desc) as rn  from cte2 
        group by museum_id, museum_name)

select * from cte3 where rn =1


--14)Identify the artists whose paintings are displayed in multiple countries
with cte as (
       select a.artist_id,a.full_name ,count(country) as no_of_countries,DENSE_RANK()over(order by count(country) desc) as rn

       from artist as a 
       inner join work w on a.artist_id=w.artist_id
       inner join museum as m on m.museum_id=w.museum_id
	   group by a.artist_id,a.full_name
	   )  select * from cte where rn =1;


--15. Display the country and the city with most no of museums. Output 2 seperate
--columns to mention the city and country. If there are multiple value, seperate them with comma

select 
    country,
    STRING_AGG(city, ' ,')  as City 
from (
        select country, city,
         count(museum_id) as museum_count,
         dense_rank()over(order by count(museum_id) desc) as rnk 
        from museum
        group by country,city
      )as q 
where 
    rnk=1
group by 
    country


---16) Identify the artist and the museum where the most expensive and least expensive painting is placed. 
--Display the artist name, sale_price, painting name, museum name, museum city and canvas label

select ar.full_name,a.work_id as painting_id,a.sale_price,w.name as painting_name,m.name as museum_name,m.city,cs.label from
(
      select * from (
       select *,DENSE_RANK()over(order by sale_price desc) as most_exp,
	   dense_rank()over(order by sale_price asc) as least_exp from product_size) as w where w.most_exp=1 or w.least_exp=1
) as a inner join work as w on a.work_id=w.work_id
       inner join artist as ar on ar.artist_id=w.artist_id
	   inner join museum as m on m.museum_id=w.museum_id
	   inner join canvas_size as cs on cs.size_id=a.size_id;


---17. Which country has the 5th highest no of paintings?

with cte as (
        select  m.country,count(w.work_id) as no_of_paintings,
        DENSE_RANK()over(order by count(w.work_id)  desc) as rn
        from museum as m inner join work as w
        on m.museum_id=w.museum_id
        where w.museum_id is not null
        group by m.country
		)
select country,no_of_paintings from cte where rn=5


---18. Which are the 3 most popular and 3 least popular painting styles?

with cte as (
            select style,count(1) as no_of_records,
                   dense_rank()over(order by count(1) desc) as most_pop,
            	   dense_rank()over(order by count(1) asc) as least_pop 
            from work
            where style is not null
            group by style),
most_pop as (
        select style, 'most_popular' as Remarks from cte where most_pop<=3 ),
least_pop as (
        select style, 'least_popular' as Remarks from cte where least_pop<=3 )
select * from most_pop 
union 
select * from least_pop;

--alternate option

		with cte as 
		(select style, count(1) as cnt
		, rank() over(order by count(1) desc) rnk
		, count(1) over() as no_of_records
		from work
		where style is not null
		group by style)
	select style
	, case when rnk <=3 then 'Most Popular' else 'Least Popular' end as remarks 
	from cte
	where rnk <=3
	or rnk > no_of_records - 3;

--19) Which artist has the most no of Portraits paintings outside USA?. Display artist name, no of paintings and the artist nationality.

with cte as (
select  a.full_name,a.nationality,count(1)as no_of_paintings ,
             dense_rank()over(order by count(1) desc) as rn from artist as a
             inner join work as w on a.artist_id=w.artist_id
			 inner join museum as m on m.museum_id=w.museum_id
			 inner join subject as s on s.work_id=w.work_id
			 where s.subject='portraits' and
			 m.country not in ('usa')

			 group by a.full_name,a.nationality
			 )
select full_name,nationality,no_of_paintings from cte where rn=1





