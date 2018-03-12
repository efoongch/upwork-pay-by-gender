import httplib2
import oauth2
import urllib3
import types
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from causalinference import CausalModel
import httplib
import base64
import csv
from statsmodels.formula.api import ols
import statsmodels.api as sm
from collections import Counter

class UpworkCausalInference:

    def __init__(self):  
        self.data_analysis_file_name = './csv_files/formatted_analysis2_df_2017_12_12_upwork_analysis_unitedstates_allskills.csv'
        self.default_output_file_name = './analyses_output/default2_causal_inference_2017_12_12_upwork_analysis_unitedstates_allskills.txt'
        
    def main(self):
        default_dataframe = self.omit_from_dataframe("default")
        self.run_causal_inference(default_dataframe)
        
    def omit_from_dataframe(self, omit_these): 
        merged = pd.read_csv(self.data_analysis_file_name)
        
        if (omit_these == "default"):
            #merged = merged[merged['final_gender'] != 'unknown']
            #merged = merged[merged['final_gender'] != 'ambiguous']
            #merged = merged[merged['new_age_range_id'] != "0"] Not going to consider age anymore
            #merged = merged[merged['new_age_range_id'] != "error"]
            merged = merged[merged['gender_computer'].notnull()] # Looking just at gender computer 
            merged = merged[merged['gender_computer'] != None ]
            merged = merged[merged['gender_computer'] != 'unisex']
            merged = merged[merged['gender_computer'] != ''] 
            merged = merged[merged['education'] != 'None']
            merged = merged[merged['education'] != 'Other']
            merged = merged[merged['education'] != 'error']
            merged = merged[merged['work_experience'] != "error"]
            merged = merged[merged['job_category'] != "none"]
        
        return merged

    def run_causal_inference(self, merged): # Returns txt file of the causal model output 
        
        log = open(self.default_output_file_name, 'a')
        log.write("########  We have started the Causal Inference Analysis ##########" + "\n")
        log.flush()
        
        # Initialize arrays, data references, and reformat some of the columns
        
        merged['bill_rate'] = merged.bill_rate.astype('float')
        all_bill_rates = merged.bill_rate.astype('float')
        merged['work_experience'] = merged.work_experience.astype('float')
        all_work_experience = merged.work_experience
        all_education_id = merged.education_id
        #all_new_age_range_id = merged.new_age_range_id
        all_job_category_id = merged.job_category_id
        #all_genders = merged.final_gender
        all_genders = merged.gender_computer # Just looking at gender computer
        gender_array = []
        bill_rate_array = []
        all_covariates_array = []

        # Converting covariates to a matrix on a dichotomous scale
        
        def make_dichotomous_matrix(id_value, covariate, final_matrix):
            for option in list(set(covariate)):
                if (id_value == option):
                    final_matrix.append(1)
                else:
                    final_matrix.append(0)
            return final_matrix

        for gender in all_genders:
            if (gender == "male"):
                gender_array.append(0)
            elif (gender == "female"): # Female as the treatment group
                gender_array.append(1)

        for rate in all_bill_rates:
            rate = round(float(rate), 2)
            bill_rate_array.append(rate)

        for row in merged.itertuples():    
            job_category_matrix = []
            education_matrix = []
            #new_age_range_id_matrix = []
            individual_covariate_matrix = []

            job_category_matrix = make_dichotomous_matrix(row.job_category_id, all_job_category_id, job_category_matrix)
            education_matrix = make_dichotomous_matrix(row.education_id, all_education_id, education_matrix)
            #new_age_range_id_matrix = make_dichotomous_matrix(row.new_age_range_id, all_new_age_range_id, new_age_range_id_matrix)

            individual_covariate_matrix.extend(job_category_matrix)
            individual_covariate_matrix.extend(education_matrix)
            #individual_covariate_matrix.extend(new_age_range_id_matrix)
            individual_covariate_matrix.append(row.work_experience)
            all_covariates_array.append(individual_covariate_matrix)

        # Sanity checks
        print "Bill rate array length: {0}".format(len(bill_rate_array))
        print "Gender array length: {0}".format(len(gender_array))
        print "All covariates array length: {0}".format(len(all_covariates_array))
        
        # Create the causal model 
        Y = np.array(bill_rate_array)
        D = np.array(gender_array)
        X = np.array(all_covariates_array)
        # np.seterr(divide='ignore', invalid='ignore')
        
        causal = CausalModel(Y, D, X)
        print "We've made the Causal Model!"
        log.write("We've made the Causal Model!" + "\n")

        log.write("---ORIGINAL STATS--- " + "\n")
        log.write(str(causal.summary_stats) + "\n")
        
        log.write("---MATCHING---" + "\n")
        causal.est_via_matching(bias_adj=True)
        print "We finished matching!!"
        log.write(str(causal.estimates) + "\n")
        log.write(str(causal.summary_stats) + "\n")    

        log.write("---PROPENSITY SCORES---" + "\n")
        causal.est_propensity_s()
        print "We finished estimating propensity scores!!"
        log.write(str(causal.propensity) + "\n")
        log.write(str(causal.summary_stats) + "\n")
        
        log.write("---TRIMMING---" + "\n")
        causal.trim_s()
        causal.cutoff
        print "We finished trimming!!"
        log.write(str(causal.summary_stats) + "\n")
        
        log.write("---STRATIFYING---" + "\n")
        causal.stratify_s()
        print "We finished stratifying!!"
        log.write(str(causal.strata) + "\n")
        log.write(str(causal.summary_stats) + "\n")
        
        log.write("---TREATMENT ESTIMATES (AFTER TRIMMING AND STRATIFYING)---" + "\n")
        causal.est_via_matching(bias_adj=True)
        print "We finished estimating via matching (after trimming)!!"
        log.write(str(causal.estimates) + "\n")
        log.write(str(causal.summary_stats) + "\n")

        print "We are all done with the causal inference analysis!"
        log.flush()

myObject = UpworkCausalInference()
myObject.main()