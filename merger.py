#!/usr/bin/env python

# To run from terminal:
# python merger_rebuild.py 'list of input files file name' 'input config file name' 'output config file name'

import pandas as pd
import numpy as np
import os
import sys
from datetime import date, timedelta

pd.options.mode.chained_assignment = None

## FUNCTIONS ##

# logs an error message and prints an alert
def log_error(message, python_message, critical):
    global error_log
    global error_count
    error_count += 1
    error_log.write('\n' + message + '\n' + python_message)
    if(critical):
        print('CRITICAL ERROR: Check error_log.txt')
        error_log.close()
        sys.exit()
    else:
        print('ERROR: Check error_log.txt')

# sorts the input config file by the output table names and the output column names
def sort_input_config_file(in_config):
    try:
        in_config = in_config[['output_tn', 'output_cn', 'form_precedence', 'input_form_name', 'input_field_name', 'date_markers', 'comparison_type']]
    except Exception as e:
        log_error('Error: The input config file does not contain the required columns', e.message, True)
    try:
        in_config['form_precedence'] = in_config['form_precedence'].astype('int')
    except Exception as e:
        log_error('Error: Not all forms have been assigned a form_precedence value', e.message, True)
    in_config = in_config.sort_values(by=['output_tn', 'output_cn'])
    return in_config

# sorts the output config file by the output table names and the output display order
def sort_output_config_file(out_config):
    try:
        out_config = out_config[['output_tn', 'output_cn', 'key_column', 'output_display_order']]
    except Exception as e:
        log_error('Error: The output config file does not contain the required columns', e.message, True)
    try:
        out_config['output_display_order'] = out_config['output_display_order'].astype('int')
    except Exception as e:
        log_error('Error: Not all columns have been assigned an output_display_order value', e.message, True)
    out_config = out_config.sort_values(by=['output_tn', 'output_display_order'])
    return out_config

# pass in config file and get out DataFrame
def load_config_file(cfile):
    try:
        cdf = pd.read_csv(cfile, dtype='string')
        return cdf
    except Exception as e:
        log_error('Error: Problem reading the config file \'' + cfile + '\'', e.message, True)

# reads the .txt file listing the directories of the required input files and loads them into a dictionary
def load_input_files(list_of_files):
    global all_input_forms
    try:
        all_input_forms = {}
        text_file = open(list_of_files)
        converted_string = text_file.read()
        file_list = converted_string.split('\n')
        for form in file_list:
            split_line = form.split('|')
            if(len(split_line) == 3):
                all_input_forms[split_line[1]] = pd.read_csv(split_line[2],dtype='string')
        return all_input_forms
    except Exception as e:
        log_error('Error: Could not load input source files from the given paths', e.message, True)

# creates an output file and adds it to the dictionary of all output files
def create_output_file(output_tn, output_files_dict, key_values):
    print('\n\n# Creating output table: \'' + output_tn + '\'\n')
    try:
        output_file_names.append(output_tn)
        output_file = 'output file'
        converter = {}
        temp = output_file
        converter[temp] = output_tn
        output_files_dict[converter[output_file]] = pd.DataFrame(columns = key_values)
        return output_files_dict
    except Exception as e:
        log_error('Error: Could not create the output table \'' + output_tn + '\'', e.message, True)

# gets the key columns for a certain output table
def get_key_columns(output_config_specific_tn):
    try:
        key_columns_df = output_config_specific_tn[['key_column']]
        key_columns_df = key_columns_df.dropna(how='any',axis=0)
        key_column_list = []
        for i, row in key_columns_df.iterrows():
            key_column_list.append(row['key_column'])
        return key_column_list
    except Exception as e:
        log_error('Error: Could not retrieve the key columns for output table \'' + output_config_specific_tn + '\'', e.message, True)

# goes through each input form required for the specific output table and makes sure they contain the required key values
def check_input_forms_for_key_values(output_tn, key_values):
    global input_config
    try:
        output_table_df = input_config[input_config.output_tn == output_tn]
        output_table_input_files_col = output_table_df[['input_form_name']]
        output_table_input_files_list = output_table_input_files_col['input_form_name'].tolist()
        output_table_input_files_list = list(dict.fromkeys(output_table_input_files_list))
        for form_name in output_table_input_files_list:
            # gets the names of the key values in the input form
            converted_key_values_list = convert_key_column_names(key_values, form_name, output_tn)
    except Exception as e:
        log_error('Error: Problem checking all of the input forms for \'' + output_tn + '\' for key values', e.message, True)

# gets the key column names in the input form
def convert_key_column_names(output_key_columns, form_name, output_tn):
    global input_config
    global input_files_dict
    converted_key_columns_list = []
    try:
        # creates a DataFrame from the input config by getting only the rows that correspond to a certain form and which contain a key value
        input_config_trimmed = input_config[(input_config.output_tn == output_tn) & (input_config.input_form_name == form_name)]
        for i, row in input_config_trimmed.iterrows():
            if row['output_cn'] not in output_key_columns:
                input_config_trimmed = input_config_trimmed.drop([i])
        # cycles through this DataFrame to make sure all of key columns are present
        for output_key_column in output_key_columns:
            found_key_column = False
            for i, row in input_config_trimmed.iterrows():
                if row['output_cn'] == output_key_column:
                    if found_key_column:
                        raise Exception ('Key column \'' + output_key_column + '\' listed more than once for ' + form_name)
                    found_key_column = True
                    input_files_dict[form_name] = input_files_dict[form_name].rename(columns = {row['input_field_name']:row['output_cn']})
                    converted_key_columns_list.append(row['output_cn'])
            # if a date, month, or year is missing, adding the missing_ keyword to the key columns list. These will be handled in the handle_missing_key_values method
            if found_key_column == False:
                raise Exception ('Missing key column \'' + output_key_column + '\' in form \'' + form_name)
    except Exception as e:
         log_error('Error: Problem retrieving the key column names from the input form \'' + form_name + '\'', e.message, True)
    return converted_key_columns_list

# create date columns for forms which contain day, month, and year columns, using information in the 'date_markers' column in the input config
def process_date_markers(input_config_specific_tn):
    try:
        global has_dmy_vars
        global date_col_name
        global day_col_name
        global month_col_name
        global year_col_name
        global input_files_dict
        has_dmy_vars = False
        # lists which store the names of the forms which contain date, day, month, and year variable columns
        forms_with_date_var = []
        forms_with_day_var = []
        forms_with_month_var = []
        forms_with_year_var = []
        # dictionaries which match a form to the form variable name for either the date, day, month, or year
        form_name_to_date_var_dict = {}
        form_name_to_day_var_dict = {}
        form_name_to_month_var_dict = {}
        form_name_to_year_var_dict = {}
        # makes sure that date markers have not been assigned to more than one output column
        old_date_name = ''
        old_day_name = ''
        old_month_name = ''
        old_year_name = ''
        # cycle through and see which forms already contain a date column, and which ones contain seperate day, month, and year values
        for i, row in input_config_specific_tn.iterrows():
            if row['date_markers'] == 'date':
                if((old_date_name != '') & (old_date_name != row['output_cn'])):
                    raise Exception ('The date marker \'date\' has been assigned to more than one output column')
                date_col_name = row['output_cn']
                old_date_name = row['output_cn']
                forms_with_date_var.append(row['input_form_name'])
                form_name_to_date_var_dict.update({row['input_form_name'] : row['input_field_name']})
            elif row['date_markers'] == 'day':
                if((old_day_name != '') & (old_day_name != row['output_cn'])):
                    raise Exception ('The date marker \'day\' has been assigned to more than one output column')
                day_col_name = row['output_cn']
                old_day_name = row['output_cn']
                forms_with_day_var.append(row['input_form_name'])
                form_name_to_day_var_dict.update({row['input_form_name'] : row['input_field_name']})
            elif row['date_markers'] == 'month':
                if((old_month_name != '') & (old_month_name != row['output_cn'])):
                    raise Exception ('The date marker \'month\' has been assigned to more than one output column')
                month_col_name = row['output_cn']
                old_month_name = row['output_cn']
                forms_with_month_var.append(row['input_form_name'])
                form_name_to_month_var_dict.update({row['input_form_name'] : row['input_field_name']})
            elif row['date_markers'] == 'year':
                if((old_year_name != '') & (old_year_name != row['output_cn'])):
                    raise Exception ('The date marker \'year\' has been assigned to more than one output column')
                year_col_name = row['output_cn']
                old_year_name = row['output_cn']
                forms_with_year_var.append(row['input_form_name'])
                form_name_to_year_var_dict.update({row['input_form_name'] : row['input_field_name']})
        forms_with_date_var.sort()
        forms_with_day_var.sort()
        forms_with_month_var.sort()
        forms_with_year_var.sort()
        # if no forms have individual day, month, and year variables, set the global boolean to False
        if ((len(forms_with_day_var) == 0) & (len(forms_with_month_var) == 0) & (len(forms_with_year_var) == 0)):
            has_dmy_vars = False
        # if the forms which contain day, month, and year variables differ, throw an error, as if a form has one of these variables it should have all of the others
        elif ((forms_with_day_var != forms_with_month_var) | (forms_with_day_var != forms_with_year_var)):
            raise Exception ('Forms with date information in seperate columns must have a column for day, month, and year. One or more of these columns are missing. Forms with columns for days: ' + str(forms_with_day_var) + '
Forms with columns for months: ' + str(forms_with_month_var) + ' Forms with columns for years: ' + str(forms_with_year_var))
        # if all forms contain one of each day, month, and year variable, create a date variable for them if they don't already have one
        else:
            has_dmy_vars = True
            # get all the forms which have day, month, and year variables but no date variable
            forms_with_only_dmy_var = list(set(forms_with_day_var).difference(set(forms_with_date_var)))
            # get all the forms which have date variables but no day, month, and year variables
            forms_with_only_date_var = list(set(forms_with_date_var).difference(set(forms_with_day_var)))
            # cycle through all of the forms with just day, month, and year variables and create date variable columns for them
            for form_name in forms_with_only_dmy_var:
                input_files_dict[form_name] = convert_dmy_to_date(form_name, input_files_dict[form_name].copy(), form_name_to_day_var_dict[form_name], form_name_to_month_var_dict[form_name], form_name_to_year_var_dict[form_nam
e])
            # cycle through all of the forms with just a date variable and create day, month, and year variable columns for them
            for form_name in forms_with_only_date_var:
                input_files_dict[form_name] = convert_date_to_day(form_name, input_files_dict[form_name].copy(), form_name_to_date_var_dict[form_name])
                input_files_dict[form_name] = convert_date_to_month(form_name, input_files_dict[form_name].copy(), form_name_to_date_var_dict[form_name])
                input_files_dict[form_name] = convert_date_to_year(form_name, input_files_dict[form_name].copy(), form_name_to_date_var_dict[form_name])
    except Exception as e:
        log_error('Error: Problem parsing through the \'date markers\' column in the input configuration file', e.message, True)

# converts a date column into a column of days
def convert_date_to_day(form_name, form_df, date_var_name):
    try:
        global day_col_name
        list_of_days = []
        for i, row in form_df.iterrows():
            list_of_days.append(row[date_var_name].split('/')[1])
        kwargs = {day_col_name + '_' + form_name : list_of_days}
        new_form_df = form_df.assign(**kwargs)
        return new_form_df
    except Exception as e:
        log_error('Error: Problem converting a column of dates into a day variable', e.message, False)

# converts a date column into a column of months
def convert_date_to_month(form_name, form_df, date_var_name):
    try:
        global month_col_name
        list_of_months = []
        for i, row in form_df.iterrows():
            list_of_months.append(row[date_var_name].split('/')[0])
        kwargs = {month_col_name + '_' + form_name : list_of_months}
        new_form_df = form_df.assign(**kwargs)
        return new_form_df
    except Exception as e:
        log_error('Error: Problem converting a column of dates into a month variable', e.message, False)

# converts a date column into a column of years
def convert_date_to_year(form_name, form_df, date_var_name):
    try:
        global year_col_name
        list_of_years = []
        for i, row in form_df.iterrows():
            list_of_years.append(row[date_var_name].split('/')[2])
        kwargs = {year_col_name + '_' + form_name : list_of_years}
        new_form_df = form_df.assign(**kwargs)
        return new_form_df
    except Exception as e:
        log_error('Error: Problem converting a column of dates into a year variable', e.message, False)

# converts day, month, and year columns into a single date column
def convert_dmy_to_date(form_name, form_df, day_var_name, month_var_name, year_var_name):
    try:
        global date_col_name
        list_of_dates = []
        for i, row in form_df.iterrows():
            list_of_dates.append(str(row[month_var_name]) + '/' + str(row[day_var_name]) + '/' + str(row[year_var_name]))
        kwargs = {date_col_name + '_' + form_name : list_of_dates}
        new_form_df = form_df.assign(**kwargs)
        return new_form_df
    except Exception as e:
        log_error('Error: Problem converting the day, month, and year columns into a date column', e.message, False)

# creates a DataFrame for a source form containing all of the key values and a unique column to be compared. Returns a list of these DataFrames.
def create_dataframes_for_each_form(unique_form_rows, key_values, table_name):
    global input_files_dict
    form_dataframes = {}
    try:
        # cycles through each row in the passed in DataFrame. Each row is a unique input form for which a new DataFrame needs to be created.
        for i, unique_form_row in unique_form_rows.iterrows():
            form_df = None
            key_values_copy = key_values[:]
            key_values_copy.append(unique_form_row['input_field_name'])
            form_df = input_files_dict[unique_form_row['input_form_name']][key_values_copy]
            if(form_df is not None):
                form_dataframes.update({unique_form_row['input_form_name']:form_df})
    except Exception as e:
        log_error('Error: One of the variables: ' +  ', '.join(str(x) for x in converted_key_values_list) + ' is not contained in the form \'' + unique_form_row['input_form_name'] + '\'. Column \'' + unique_form_row['output_cn
'] + '\' can not be compared across forms', e.message, False)
        # if everything went smoothly, a DataFrame will have been created which contains all of the key values and unique column for a specific form. Add this to the list.
    return form_dataframes

# creates a dictionary which matches each form to a precedence value
def get_form_precedence_dict(unique_form_rows):
    form_precedence_dict = {}
    try:
        for i, unique_form_row in unique_form_rows.iterrows():
            form_precedence_dict.update({unique_form_row['input_form_name']:unique_form_row['form_precedence']})
    except Exception as e:
        log_error('Error: Could not find a form precedence value for all forms', e.message, False)
    return form_precedence_dict

# creates a dictionary which matches each var to a form name
def get_var_to_form_dict(unique_form_rows):
    var_to_form_dict = {}
    try:
        for i, unique_form_row in unique_form_rows.iterrows():
            var_to_form_dict.update({unique_form_row['input_field_name']:unique_form_row['input_form_name']})
    except Exception as e:
        log_error('Error: Could not match all input variable names to form names', e.message, False)
    return var_to_form_dict

# compares the last column of each DataFrame in form_dataframes and checks for discrepancies between them
def find_discrepancies(discrepancies_list, form_dataframes, key_values, output_val, output_tn, form_precedence_dict, var_to_form_dict, rule):
    merged_df = None
    form_name_list = []
    try:
        # merges all forms in form_dataframes by their key values
        for form_name, form in form_dataframes.iteritems():
            form_name_list.append(form_name)
            if merged_df is None:
                merged_df = form
            else:
                merged_df = pd.merge(merged_df, form, how = 'outer', on = key_values)
            if form_name not in discrepancies_list.columns:
                discrepancies_list.insert(loc = len(discrepancies_list.columns), column=form_name, value=['' for i in range(discrepancies_list.shape[0])])
        # creates a column for the 'winning val', which is the value that will be chosen after comparing the different input values
        merged_df['winning_val'] = 'nan'
        # for loop cycles through each row, compares the input values, and decides a 'winner'; if there is a discrepancy write it to the discrepancy DataFrame
        for i, merged_row in merged_df.iterrows():
            discrepancy_found = False
            var_name_to_val_dict = {}
            winning_val = 'nan'
            for j in range(0, len(form_dataframes)):
                var_name_to_val_dict.update({merged_df.columns[len(merged_df.columns)-(j+2)]:str(merged_row[merged_df.columns[len(merged_df.columns)-(j+2)]])})
            new_var_name_to_val_dict = var_name_to_val_dict.copy()
            # gets rid of null values
            for val_name, val in var_name_to_val_dict.iteritems():
                if ((val == 'nan') | (val == '') | (val == '-4') | (pd.isnull(val))):
                    del new_var_name_to_val_dict[val_name]
            highest_precedence_val = 'nan'
            # determines the highest precedence value; if another value exists with equal precedence and is different, log the discrepancy
            if len(new_var_name_to_val_dict) > 1:
                is_first_val = True
                highest_precedence = 9
                for val_name, val in new_var_name_to_val_dict.iteritems():
                    # set the first val in the dictionary of values as the winner, set the current highest precedence to this form precedence
                    if is_first_val:
                        winning_val = val
                        highest_precedence_val = val
                        highest_precedence = int(form_precedence_dict[var_to_form_dict[val_name]])
                        is_first_val = False
                    # for the rest of the vals in the dictionary, compare their precedence to the first val; if higher, set the highest precedence and winner to the current val
                    else:
                        current_val_precedence = int(form_precedence_dict[var_to_form_dict[val_name]])
                        if(current_val_precedence < highest_precedence):
                            winning_val = val
                            highest_precedence_val = val
                            highest_pecedence = current_val_precedence
                            discrepancy_found = False
                        # if the current value has the same precedence as the highest found precedence and the values don't match, a discrepancy has been found (unless a value with a higher discrepancy is found later)
                        elif((current_val_precedence == highest_precedence) & (val != highest_precedence_val)):
                            # pass the values to the rule filter to see if a discrepancy still exists based on the rule given
                            if(has_discrepancy_after_rule(val, highest_precedence_val, rule)):
                                discrepancy_found = True
            # if there is only one value, set it as the winner
            elif len(new_var_name_to_val_dict) == 1:
                winning_val = list(new_var_name_to_val_dict.values())[0]
           # if a discrepancy is found, add it to the discrepancy spreadsheet and put 'discrep' in place of a value in the output table
            if discrepancy_found == True:
                print('! Discrepancy found, written to discrepancies.csv !')
                winning_val = 'discrep'
                discrept_dict = {'output_tn' : output_tn, 'output_cn' : output_val}
                for key_val in key_values:
                    discrept_dict.update({key_val : merged_row[key_val]})
                for k in range(0, len(form_dataframes)):
                    discrept_dict.update({form_name_list[len(form_name_list)-(k+1)] : str(merged_row[merged_df.columns[len(merged_df.columns)-(k+2)]])})
                discrepancies_list = discrepancies_list.append(discrept_dict, ignore_index = True)
            merged_df.at[i, 'winning_val'] = winning_val
        count2 = 0
        # add all the winning values to the output table
        while count2 < len(form_dataframes):
            del merged_df[merged_df.columns[len(key_values)]]
            count2 += 1
        merged_df.rename(columns={'winning_val': output_val}, inplace = True)
        add_value_to_output_table(output_tn, output_val, merged_df, key_values)
    except Exception as e:
        log_error('Error: Problem with comparing values across input forms for ' + output_val, e.message, False)
    return discrepancies_list

# adds the value to the output table
def add_value_to_output_table(output_tn, output_val, new_col_df, key_values):
    try:
        global output_files_dict
        global date_col_name
        global day_col_name
        global month_col_name
        global year_col_name
        global has_dmy_vars
        # special case: if the column is the column of dates, split it up into columns for day, month, and year and merge these as well
        if ((has_dmy_vars) & (output_val == date_col_name)):
            day_col = []
            month_col = []
            year_col = []
            for i, row in new_col_df.iterrows():
                if(row[output_val] == 'discrep'):
                    day_col.append('discrep')
                    month_col.append('discrep')
                    year_col.append('discrep')
                else:
                    day_col.append(row[output_val].split('/')[1])
                    month_col.append(row[output_val].split('/')[0])
                    year_col.append(row[output_val].split('/')[2])
            new_col_df[day_col_name] = day_col
            new_col_df[month_col_name] = month_col
            new_col_df[year_col_name] = year_col
        # merges the column of winning vals with the rest of the output table
        output_files_dict[output_tn] = pd.merge(output_files_dict[output_tn], new_col_df, how = 'outer', on = key_values)
    except Exception as e:
        log_error('Error: Problem with adding the values to the output table', e.message, False)

# if the two values are still different after having been modified by the given rule, return true
def has_discrepancy_after_rule(val1, val2, rule):
    try:
        # ADD DISCREPANCY RULES HERE

        if(rule == 'nan'):
            return True

        ## Date rules: if the dates are within a certain time period, there is no discrepancy.
        ## Examples: 'date_90_day' - dates within 90 days of each other are considered OK
        ##           'date_6_month' - within 6 months OK (a month is classified as 30 days)
        ##           'date_2_year' - within 2 years OK (a year is classified as 365 days)
        if(rule[0:4] == 'date'):
            date_rule = rule.split('_')
            date1 = date(int(val1.split('/')[2]), int(val1.split('/')[0]), int(val1.split('/')[1]))
            date2 = date(int(val2.split('/')[2]), int(val2.split('/')[0]), int(val2.split('/')[1]))
            if(date_rule[2] == 'day'):
                if date1 - date2 <= timedelta(days = int(date_rule[1])):
                    return False
            elif(date_rule[2] == 'month'):
                if date1 - date2 <= timedelta(days = (30 * int(date_rule[1]))):
                    return False
            elif(date_rule[2] == 'year'):
                if date1 - date2 <= timedelta(days = (365 * int(date_rule[1]))):
                    return False

        ## Compares floats and ints
        ## Example: '1.0' is the same as '1'
        if(rule == 'compare_value_int'):
            float1 = float(val1)
            float2 = float(val2)
            if(float1 == float2):
                return False

    except Exception as e:
        log_error('Error: Problem with comparing values based on given rule: ' + str(rule), e.message, False)
    return True


## START OF PROGRAM ##

# creates a log of errors regarding the format of the given forms, the program, etc.
if(os.path.exists('error_log.txt')):
    os.remove('error_log.txt')
error_log = open('error_log.txt', 'a')

# counts the number of errors handled while running
error_count = 0

# loads and sorts the config files
input_config = load_config_file(sys.argv[2])
input_config = sort_input_config_file(input_config)
output_config = load_config_file(sys.argv[3])
output_config = sort_output_config_file(output_config)

# loads the list of input files
input_files_dict = load_input_files(sys.argv[1])

# list of output file names to keep track of which ones have already been created
output_file_names = []

# dictionary which stores the output file DataFrames
output_files_dict = {}

# list of strings containing the key values for each output file
key_values_list = []

# DataFrame which contains all of the discrepancies
discrepancies_list = pd.DataFrame()

# boolean which stores whether the discrepancies list has been created yet
discrepancies_list_made = False

# boolean which stores whether the current output table has variables for the individual day, month and year variables
has_dmy_vars = False

# the name of the date, day, month, and year output columns. These names will be overriden to the names in the output config if the date markers are assigned.
date_col_name = 'visdate'
day_col_name = 'visday'
month_col_name = 'vismonth'
year_col_name = 'visyear'

print('Starting...')

# for loop which iterates through the output config file
for i, row in output_config.iterrows():

    # if the output table has not been created yet, create it
    if row['output_tn'] not in output_file_names:

        # get all the key column values for the new output table
        old_key_values = key_values_list[:]
        key_values_list = get_key_columns(output_config[output_config.output_tn == row['output_tn']])

        # create a DataFrame for the output table and add it to the dictionary storing all of the output tables
        output_files_dict = create_output_file(row['output_tn'], output_files_dict, key_values_list)

        # goes through all of the input forms needed to create the output table and makes sure they contain the required key values
        check_input_forms_for_key_values(row['output_tn'], key_values_list)

        # process the date markers column: create date columns for forms which have seperate day, month, and year columns
        process_date_markers(input_config[input_config.output_tn == row['output_tn']])

        # updates the discrepancies list to include new key values, e.g. ones that haven't been seen in a previous output table
        if discrepancies_list_made:
            for val in key_values_list:
                if val not in old_key_values:
                    discrepancies_list.insert(loc=2, column=val, value=['' for i in range(discrepancies_list.shape[0])])

        # create the discrepancies list if not made yet
        if discrepancies_list_made == False:
            discrepancies_list_cols = ['output_tn', 'output_cn'] + key_values_list
            discrepancies_list = pd.DataFrame(columns = discrepancies_list_cols)
            discrepancies_list_made = True

    # for each output column name, retrieve all the rows from the input config file which correspond to this output table and column name
    input_config_matching_rows = input_config[(input_config.output_tn == row['output_tn']) & (input_config.output_cn == row['output_cn'])]

    # if this DataFrame is empty, this means there is no information about this variable in the input form and a critical error should be thrown
    if(input_config_matching_rows.empty):
        log_error('Error: No corresponding input column for the output column \'' + row['output_cn'] + '\' in the input config file', '', True)

    # special case for the date column: compare across forms not just listed with a date output column, but also those with day, month, and year output columns, as a date column has been generated for these forms
    if(input_config_matching_rows.iloc[0]['date_markers'] == 'date'):
        input_config_matching_rows = input_config[(input_config.output_tn == row['output_tn']) & ((input_config.date_markers == 'date') | (input_config.date_markers == 'day'))]
        for j, input_config_matching_row in input_config_matching_rows.iterrows():
            if input_config_matching_row['output_cn'] != date_col_name:
                input_config_matching_rows.at[j, 'output_cn'] = date_col_name
                input_config_matching_rows.at[j, 'input_field_name'] = date_col_name + '_' + input_config_matching_row['input_form_name']

    # if more than one row exists, this means that there are multiple source/input forms for this column value and they must be compared. Don't compare if this column value is a key value
    if(row['output_cn'] not in key_values_list):

        print('\n## Retrieving value for \'' + row['output_cn'] + '\'')
        log_error('## Retrieving value for \'' + row['output_cn'] + '\'', '', False)

        # creates DataFrames for each input form which includes the key values and the current column value
        form_dataframes = create_dataframes_for_each_form(input_config_matching_rows, key_values_list, row['output_tn'])
        form_precedence_dict = get_form_precedence_dict(input_config_matching_rows)
        var_to_form_dict = get_var_to_form_dict(input_config_matching_rows)

        # gets the comparison rule for the current column value
        rule = str(input_config_matching_rows.iloc[0]['comparison_type'])

        # once these DataFrames have been created, compare their values, check for discrepancies, and export to the output table
        if(len(form_dataframes) != 0):
            print('...comparing ' + str(len(form_dataframes)) + ' forms')
            if((row['output_cn'] != day_col_name) & (row['output_cn'] != month_col_name) & (row['output_cn'] != year_col_name)):
                discrepancies_list = find_discrepancies(discrepancies_list, form_dataframes, key_values_list, row['output_cn'], row['output_tn'], form_precedence_dict, var_to_form_dict, rule)

# export each output file to a .csv with columns in the display order
for output_file_name in output_files_dict:
    output_config_matching_table = output_config[(output_config.output_tn == output_file_name)].sort_values(by=['output_display_order'])
    in_order_output_table = pd.DataFrame()
    for i, row in output_config_matching_table.iterrows():
        in_order_output_table[row['output_cn']] = output_files_dict[output_file_name][row['output_cn']].copy()
    in_order_output_table.to_csv(output_file_name + '.csv', index = False)

discrepancies_list.to_csv('discrepancies.csv', index = False)

error_log.close()

print('\nComplete.\nFinished with ' + str(error_count) + ' error(s).\nErrors listed in error_log.txt.\nDiscrepancies listed in discrepancies.csv.\n' + str(len(output_files_dict)) + ' output files created: ' + str(output_files_dict.keys()))
