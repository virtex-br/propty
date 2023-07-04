create table epc (
    LMK_KEY TEXT,
    ADDRESS1 TEXT,
    ADDRESS2 TEXT,
    ADDRESS3 TEXT,
    POSTCODE TEXT,
    BUILDING_REFERENCE_NUMBER NUMERIC,
    CURRENT_ENERGY_RATING TEXT,
    POTENTIAL_ENERGY_RATING TEXT,
    CURRENT_ENERGY_EFFICIENCY TEXT,
    POTENTIAL_ENERGY_EFFICIENCY TEXT,
    PROPERTY_TYPE TEXT,
    BUILT_FORM TEXT,
    INSPECTION_DATE DATE,
    LOCAL_AUTHORITY TEXT,
    CONSTITUENCY TEXT,
    COUNTY TEXT,
    LODGEMENT_DATE DATE,
    TRANSACTION_TYPE TEXT,
    ENVIRONMENT_IMPACT_CURRENT TEXT,
    ENVIRONMENT_IMPACT_POTENTIAL TEXT,
    ENERGY_CONSUMPTION_CURRENT TEXT,
    ENERGY_CONSUMPTION_POTENTIAL TEXT,
    CO2_EMISSIONS_CURRENT TEXT,
    CO2_EMISS_CURR_PER_FLOOR_AREA TEXT,
    CO2_EMISSIONS_POTENTIAL TEXT,
    LIGHTING_COST_CURRENT TEXT,
    LIGHTING_COST_POTENTIAL TEXT,
    HEATING_COST_CURRENT TEXT,
    HEATING_COST_POTENTIAL TEXT,
    HOT_WATER_COST_CURRENT TEXT,
    HOT_WATER_COST_POTENTIAL TEXT,
    TOTAL_FLOOR_AREA NUMERIC,
    ENERGY_TARIFF TEXT,
    MAINS_GAS_FLAG TEXT,
    FLOOR_LEVEL TEXT,
    FLAT_TOP_STOREY TEXT,
    FLAT_STOREY_COUNT TEXT,
    MAIN_HEATING_CONTROLS TEXT,
    MULTI_GLAZE_PROPORTION TEXT,
    GLAZED_TYPE TEXT,
    GLAZED_AREA TEXT,
    EXTENSION_COUNT TEXT,
    NUMBER_HABITABLE_ROOMS TEXT,
    NUMBER_HEATED_ROOMS TEXT,
    LOW_ENERGY_LIGHTING TEXT,
    NUMBER_OPEN_FIREPLACES TEXT,
    HOTWATER_DESCRIPTION TEXT,
    HOT_WATER_ENERGY_EFF TEXT,
    HOT_WATER_ENV_EFF TEXT,
    FLOOR_DESCRIPTION TEXT,
    FLOOR_ENERGY_EFF TEXT,
    FLOOR_ENV_EFF TEXT,
    WINDOWS_DESCRIPTION TEXT,
    WINDOWS_ENERGY_EFF TEXT,
    WINDOWS_ENV_EFF TEXT,
    WALLS_DESCRIPTION TEXT,
    WALLS_ENERGY_EFF TEXT,
    WALLS_ENV_EFF TEXT,
    SECONDHEAT_DESCRIPTION TEXT,
    SHEATING_ENERGY_EFF TEXT,
    SHEATING_ENV_EFF TEXT,
    ROOF_DESCRIPTION TEXT,
    ROOF_ENERGY_EFF TEXT,
    ROOF_ENV_EFF TEXT,
    MAINHEAT_DESCRIPTION TEXT,
    MAINHEAT_ENERGY_EFF TEXT,
    MAINHEAT_ENV_EFF TEXT,
    MAINHEATCONT_DESCRIPTION TEXT,
    MAINHEATC_ENERGY_EFF TEXT,
    MAINHEATC_ENV_EFF TEXT,
    LIGHTING_DESCRIPTION TEXT,
    LIGHTING_ENERGY_EFF TEXT,
    LIGHTING_ENV_EFF TEXT,
    MAIN_FUEL TEXT,
    WIND_TURBINE_COUNT TEXT,
    HEAT_LOSS_CORRIDOR TEXT,
    UNHEATED_CORRIDOR_LENGTH TEXT,
    FLOOR_HEIGHT NUMERIC,
    PHOTO_SUPPLY TEXT,
    SOLAR_WATER_HEATING_FLAG TEXT,
    MECHANICAL_VENTILATION TEXT,
    ADDRESS TEXT,
    LOCAL_AUTHORITY_LABEL TEXT,
    CONSTITUENCY_LABEL TEXT,
    POSTTOWN TEXT,
    CONSTRUCTION_AGE_BAND TEXT,
    LODGEMENT_DATETIME TEXT,
    TENURE TEXT,
    FIXED_LIGHTING_OUTLETS_COUNT TEXT,
    LOW_ENERGY_FIXED_LIGHT_COUNT TEXT,
    UPRN TEXT,
    UPRN_SOURCE TEXT
);
create index on epc(postcode);

create table sales (
    unique_id text,
    price_paid numeric,
    deed_date date,
    postcode text,
    property_type char,
    new_build boolean,
    estate_type char,
    saon text,
    paon text,
    street text,
    locality text,
    town text,
    district text,
    county text,
    transaction_category char,
    field_a char
);
create index on sales(postcode);

create table postcodes (
    postcode text,
    lat numeric,
    lon numeric
);
create index on postcodes(postcode);
create index on postcodes(lat, lon);

create materialized view prices as
with d as (
    select distinct
        unique_id,
        saon,
        street,
        locality,
        town,
        district,
        sales.county,
        sales.postcode,
        sales.property_type,
        deed_date,
        price_paid,
        inspection_date,
        total_floor_area,
        round(price_paid / NULLIF(total_floor_area, 0)) as ppsm,
        row_number() over(partition by unique_id order by abs(inspection_date - deed_date)) r,
    from sales
        left outer join epc on epc.postcode = sales.postcode
        and replace(address, ',', '') like concat_ws(' ', nullif(paon, ''), saon, street || '%')
    -- where sales.postcode = 'HP4 3DD' and saon = '3'
    order by deed_date,
        inspection_date)
    select * from d where r = 1;

create index on prices(postcode);


create or replace view prices_view as (
    select deed_date date, price_paid as price, total_floor_area size, ppsm, property_type, saon, street, town, county, postcode from prices
);

create view prices_view as
with d as (
    select distinct
        saon,
        street,
        locality,
        town,
        district,
        sales.county,
        sales.postcode,
        sales.property_type,
        deed_date,
        price_paid,
        inspection_date,
        total_floor_area,
        round(price_paid / NULLIF(total_floor_area, 0)) as ppsm,
        abs(inspection_date - deed_date),
        row_number() over(partition by deed_date order by abs(inspection_date - deed_date)) r
    from sales
        left outer join epc on epc.postcode = sales.postcode
        and replace(address, ',', '') like concat(case when nullif(paon, '') is not null then paon || ' ' else '' end, saon, ' %')
    where sales.postcode = 'HP4 3JH'
    order by deed_date,
        inspection_date)
    select * from d where r = 1;


create or replace function get_postcodes(target_postcode text, radius numeric) returns table (postcode text, distance double precision) as $$
declare my_lat numeric;
my_lon numeric;
BEGIN
select lat,
    lon into my_lat,
    my_lon
from postcodes p
where p.postcode = target_postcode;
raise notice 'Lat: % | Long: %',
my_lat, my_lon;
BEGIN return query with d as (
    select postcodes.postcode,
        (
            3959 * acos (
                cos (radians(my_lat)) * cos(radians(lat)) * cos(radians(lon) - radians(my_lon)) + sin (radians(my_lat)) * sin(radians(lat))
            )
        ) as distance
    FROM postcodes
    ORDER BY 2
)
select *
from d
where d.distance <= radius;
END;
END;
$$ LANGUAGE 'plpgsql';


create or replace function price_by_area(target_postcode text, radius numeric) returns table (
        address text,
        postcode text,
        date date,
        price numeric,
        inspection_date date,
        property_type char,
        size numeric,
        ppsm numeric,
        distance numeric
    ) as $$ BEGIN return query
select concat_ws(', ', saon, street, town, p.postcode) address,
    p.postcode,
    deed_date date,
    price_paid price,
    p.inspection_date,
    p.property_type,
    total_floor_area size,
    p.ppsm,
    round(pc.distance::numeric, 2) as distance
from prices p
    join get_postcodes(target_postcode, radius) pc on p.postcode = pc.postcode
order by deed_date;
END;
$$ LANGUAGE 'plpgsql';


COPY sales from '/tmp/data/pp-complete.csv' CSV;
COPY epc from '/tmp/data/epc.csv' CSV;
COPY postcodes from '/tmp/data/postcodes.csv' CSV;