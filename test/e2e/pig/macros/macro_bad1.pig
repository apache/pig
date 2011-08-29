-- macro with syntax error

define simple_macro(in_relation, min_gpa, max_age) returns c {
    b = fiter $in_relation by gpa >= $min_gpa and age <= $max_age;
    c = foreach b generate age, name;
};
