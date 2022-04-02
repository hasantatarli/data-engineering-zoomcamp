 {#
    This macro returns the description of the bicycle_type_id 
#}

{% macro get_bicycle_type_description(bicycle_type_id) -%}

    case {{ bicycle_type_id }}
        when 1 then 'Road Bike'
        when 2 then 'Mountain Bike'
        when 3 then 'Touring Bike'
        when 4 then 'Folding Bike'
        when 5 then 'Fixed Gear/ Track Bike'
        when 6 then 'BMX'
        when 7 then 'Recumbent Bike'
        when 8 then 'Cruiser'
        when 9 then 'Hybrid Bike'
        when 10 then 'Cyclocross Bike'
        when 11 then 'Electric Bike'
    end

{%- endmacro %}