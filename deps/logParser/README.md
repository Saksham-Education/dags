# log_parser

### Prerequisites
This DAG require below python dependencies. Make sure all the dependencies are already installed.

* lars==1.0
* pandas==1.2.3
* numpy==1.20.1
* SQLAlchemy==1.3.15
* psycopg2-binary==2.9.1
* urllib3==1.26.6
* beautifulsoup4==4.9.3

### Database Table Schema:
```
-- Table: public.dashboard_event_filter

-- DROP TABLE public.dashboard_event_filter;

CREATE TABLE public.dashboard_event_filter
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    filter character varying(255) COLLATE pg_catalog."default",
    value character varying(255) COLLATE pg_catalog."default",
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    request_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT dashboard_event_filter_pkey PRIMARY KEY (id),
    CONSTRAINT log_dashboard_event_id_fkey FOREIGN KEY (request_id)
        REFERENCES public.log_dashboard_events (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE public.dashboard_event_filter
    OWNER to postgres;


-- Table: public.log_dashboard_events

-- DROP TABLE public.log_dashboard_events;

CREATE TABLE public.log_dashboard_events
(
    remote_host character varying(255) COLLATE pg_catalog."default" NOT NULL,
    request_time time without time zone,
    dashboard_id character varying(255) COLLATE pg_catalog."default",
    method character varying(10) COLLATE pg_catalog."default",
    request_path_str character varying(255) COLLATE pg_catalog."default",
    request_query_str text COLLATE pg_catalog."default",
    status integer,
    req_referer_query_str text COLLATE pg_catalog."default",
    req_user_agent character varying(510) COLLATE pg_catalog."default",
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at time without time zone,
    id character varying(255) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT log_dashboard_events_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE public.log_dashboard_events
    OWNER to postgres;
```

### Deployment steps:
1. Added above python packages in the `requirements.txt` file of airflow repository so make sure to rebuild the docker container so the new container have required dependencies.
2. Log server URL is defined in the `utility.py` file, please change the URL is it is different for the production. Current URL is `http://68.183.85.218:8000/`.
3. Database credentials are defined in the vault with name `log_parser`, change credentials for production if database is different for production.
