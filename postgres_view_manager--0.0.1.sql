/*
 * Postgres view manager makes it easier to create and replace views
 * in Postgres without having to constantly drop and recreate them.
 *
 * Rationale: I hate nested views.
 */
/*
 * Setup
 */
alter default privileges revoke execute on functions from public;
alter default privileges revoke all on tables from public;

create schema if not exists pgvm;

/*
 * Tables
 */
create table if not exists pgvm.view (
    id bigint generated always as identity primary key,
    schema text not null default 'public',
    name text not null,
    definition text not null,
    materialized boolean not null default false,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now(),
    unique (schema, name)
);

create table if not exists pgvm.view_history (
    id bigint generated always as identity primary key,
    view_id bigint not null references pgvm.view(id),
    definition text not null,
    materialized boolean not null default false,
    created_at timestamp with time zone not null default now()
);

create table if not exists pgvm.stored_view (
    id bigint generated always as identity primary key,
    view_id bigint not null references pgvm.view(id),
    root_view_id bigint not null references pgvm.view(id),
    stored_at timestamp with time zone not null default now(),
    table_comment text,
    column_comments text[][], -- array of [column_name, comment]
    restored_at timestamp with time zone
);

/*
 * Functions
 */
create or replace function pgvm.run_create_view(view_id bigint)
returns void
language plpgsql
as
    $$
declare
    view_record record;
    ddl text;
begin
    select * from pgvm.view where id = view_id into view_record;

    if view_record.materialized then
        ddl := format('create materialized view %I.%I as %s', view_record.schema, view_record.name, view_record.definition);
    else
        ddl := format('create view %I.%I as %s', view_record.schema, view_record.name, view_record.definition);
    end if;

    execute ddl;
end;
$$
;

create or replace function pgvm.get_dependent_views(view_id bigint)
returns bigint[]
language plpgsql
as $$
declare
    root_view record;
    dependent_views dependent_views;
    dependent_view_ids bigint[];
begin
    create temp table dependent_views (
        obj_schema text,
        obj_name text,
        obj_type text,
        depth integer
    ) on commit drop;
    select * from pgvm.view where id = view_id into root_view;
    with recursive recursive_deps (obj_schema, obj_name, obj_type, depth) as (
        select
            root_view.schema collate "C",
            root_view.name collate "C",
            null::text collate "C",
            0
        union
        select
            dep_schema::text,
            dep_name::text,
            dep_type::text,
            depth::integer + 1
        from (
          select
              nsp.nspname as ref_schema,
              cl.relname as ref_name,
              rwr_cl.relkind as dep_type,
              rwr_nsp.nspname as dep_schema,
              rwr_cl.relname as dep_name
          from
              pg_depend as dep
              left join pg_class as cl
                  on dep.refobjid = cl.oid
              left join pg_namespace as nsp
                  on cl.relnamespace = nsp.oid
              left join pg_rewrite as rwr
                  on dep.objid = rwr.oid
              left join pg_class as rwr_cl
                  on rwr.ev_class = rwr_cl.oid
              left join pg_namespace as rwr_nsp
                  on rwr_cl.relnamespace = rwr_nsp.oid
          where dep.deptype = 'n'
              and dep.classid = 'pg_rewrite'::regclass
        ) as deps
        join recursive_deps
            on deps.ref_schema = recursive_deps.obj_schema
            and deps.ref_name = recursive_deps.obj_name
        where deps.ref_schema != deps.dep_schema
            or deps.ref_name != deps.dep_name
    )
    select
        obj_schema,
        obj_name,
        obj_type,
        depth
    into dependent_views
    from recursive_deps
    where depth > 0;
end;
$$
;

create or replace function
    pgvm.create_view(
        schema text, name text, definition text, materialized boolean default false
    )
returns bigint
language plpgsql
as
    $$
declare
    view_id bigint;
begin
    insert into pgvm.view (schema, name, definition, materialized) values (schema, name, definition, materialized)
    on conflict (name) do update set definition = definition, updated_at = now()
    returning id into view_id;

    insert into pgvm.view_history (view_id, definition, materialized) values (view_id, definition, materialized);
    
    pgvm.run_create_view(view_id);

    return view_id;
end;
$$
;

create or replace function
    pgvm.create_view(name text, definition text, materialized boolean default false)
returns bigint
language plpgsql
as $$ return pgvm.create_view('public', name, definition, materialized); $$
;
