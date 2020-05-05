select a.call_date,a.journey,a.staffgroup,a.calls, b.calls_case from
dmquerywork.u6ak_lead_rate a
full outer join (select b.call_date, b.journey, b.staffgroup, count(distinct b.callid) as calls_case from
dmquerywork.call_to_case b
group by call_date,journey,staffgroup) b
on a.call_date = b.call_date and a.journey = b.journey and a.staffgroup = b.staffgroup
where a.call_date > date('2020-04-10')
