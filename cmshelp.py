import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas import Series, DataFrame
import pytz
from pytz import common_timezones, all_timezones
import matplotlib
matplotlib.style.use('ggplot')
#%matplotlib inline
from datetime import datetime
import plotly.plotly as py
import plotly.graph_objs as go
import plotly

###########################################################################################################

def dataframe_from_url(urlstring):
    """A function to turn the CMS urls pointing to zipped files
    into dataframes. Returns a dataframe"""
    import requests, zipfile, StringIO
    r = requests.get(urlstring)
    z = zipfile.ZipFile(StringIO.StringIO(r.content))
    # need to tease out the appropiate part of the sample string
    df = pd.read_csv(z.open(urlstring.split('/')[-1].replace('zip','csv')))
    
    return df


##################################################################################################################

def get_url_list_sample_1():
    """returns a a list of all the sample 1
    urls."""
    sample1list = ["https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/" + \
               "SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip",
              "http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip",
              "http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip",
              "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/" + \
               "SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip",
              "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/" + \
               "SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip",
              "http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip",
              "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/" + \
               "SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip",
              "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/" + \
               "SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip"]

    return sample1list


###################################################################################################################


def get_url_list_sample_i(i = 1):
    """returns a list of all the sample i urls.
    value values for i are 1 through 20 inclusive."""
    orig_list = get_url_list_sample_1()
    new_list = [url.replace("_1", "_"+str(i)) for url in orig_list]
    return new_list


#######################################################################################################################


def make_geography_frames():
    """create three dataframes

    dfplace contains the lat long info.
    dfssa contains some crosswalk info
   
    ."""
    dfplace = pd.read_pickle('dfplace.pickle')
    dfssa = pd.read_table('http://www.nber.org/ssa-fips-state-county-crosswalk/CBSAtoCountycrosswalk_FY13.txt',
                     dtype={'SSA State county code': object,
                           'FIPS State county code': object})

    return dfssa, dfplace


#################################################################################################################


def make_elev_frame(dfssa, dfplace):
    """Construct the elevation from by merging the two arguments."""
    dfelev = pd.merge(dfssa, dfplace, left_on='FIPS State county code',
                 right_on='FIPScombo')
    return dfelev


######################################################################################################


def process_url(urlstring):
    """ turn the CMS url pointing to a zipped file
    into a dataframes."""
    from io import StringIO, BytesIO
    import requests, zipfile
    r = requests.get(urlstring)
    z = zipfile.ZipFile(BytesIO(r.content))
    # need to tease out the appropiate part of the sample string
    df = pd.read_csv(z.open(urlstring.split('/')[-1].replace('zip','csv')))
    
    return df



############################################################################################################


def make_clean_bene(url):
    """Make a clean dataframe corresponding to a beneficiary file.

    if url is not corresponding to a beneficiary file, raise an error
    and exit. Otherwise return a dataframe with dates properly 
    transformed and the geographical enhanced."""
    import datetime as dt

    if 'Bene' not in url:
        print('That url does not correspond to a beneficiary file!')
        return None

    dfraw = process_url(url)
    dfssa, dfplace = make_geography_frames()
    dfelev = make_elev_frame(dfssa, dfplace)

    dfraw['BENE_BIRTH_DT'] = pd.to_datetime(dfraw['BENE_BIRTH_DT'].astype(str), format='%Y%m%d')
    dfraw['BENE_DEATH_DT'] = pd.to_datetime(dfraw['BENE_DEATH_DT'].astype(str), format='%Y%m%d')
    
    
    dfraw.replace({'BENE_SEX_IDENT_CD':{1:'Male',
                                         2:'Female'}},
                                        inplace=True)
    dfraw.replace({'BENE_RACE_CD':{1: 'White',
                                   2:'Black',
                                   3: 'Others',
                                   5: 'Hispanic'}},
                                    inplace=True)
    dfraw.replace({'BENE_ESRD_IND':{'0':'The beneficiary does not have end stage renal disease',
                                'Y':'The beneficiary has end stage renal disease'}},
                                        inplace=True)
    dfraw['state'] = dfraw['SP_STATE_CODE'].copy()
    dfraw['state'] = dfraw['state'].apply(str)
    dfraw['state'] = dfraw['state'].apply(lambda x: x.rjust(2,'0'))
    dfraw.replace({'SP_STATE_CODE':{1: 'AL',
                                    2: 'AK',
                                    3: 'AZ',
                                    4: 'AR',
                                    5: 'CA',
                                    6: 'CO',
                                     7: 'CT',
                                    8: 'DE',
                                    9: 'DC',
                                    10: 'FL',
                                    11: 'GA',
                                    12: 'HI',
                                    13: 'ID',
                                    14: 'IL',
                                    15: 'IN',
                                    16: 'IA',
                                    17: 'KS',
                                    18: 'KY',
                                    19: 'LA',
                                    20: 'ME',
                                    21: 'MD',
                                    22: 'MA',
                                    23: 'MI',
                                    24: 'MN',
                                    25: 'MS',
                                    26: 'MO',
                                    27: 'MT',
                                    28: 'NE',
                                    29: 'NV',
                                    30: 'NH',
                                    31: 'NJ',
                                    32: 'NM',
                                    33: 'NY',
                                    34: 'NC',
                                    35: 'ND',
                                    36: 'OH',
                                    37: 'OK',
                                    38: 'OR',
                                    39: 'PA',
                                    41: 'RI',
                                    42: 'SC',
                                    43: 'SD',
                                    44: 'TN',
                                    45: 'TX',
                                    46: 'UT',
                                    47: 'VT',
                                    49: 'VA',
                                    50: 'WA',
                                    51: 'WV',
                                    52: 'WI',
                                    53: 'WY',
                                    54: 'Others'}},
                 inplace=True)
    dfraw['BENE_COUNTY_CD'] = dfraw['BENE_COUNTY_CD'].apply(str)
    dfraw['BENE_COUNTY_CD'] = dfraw['BENE_COUNTY_CD'].apply(lambda x: x.rjust(3,'0'))
    dfraw['crosswalk'] = dfraw['state'] + dfraw['BENE_COUNTY_CD']
    dfhuh = pd.merge(dfraw, dfelev, left_on='crosswalk', right_on='SSA State county code')
    dfhuh.replace({'SP_ALZHDMTA':{1:'Yes',
                                         2:'No'}},
                                        inplace=True)
    dfhuh.replace({'SP_CHF':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_CHRNKIDN':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_CNCR':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_COPD':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_DEPRESSN':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_DIABETES':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_ISCHMCHT':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_OSTEOPRS':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_CHRNKIDN':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_RA_OA':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    dfhuh.replace({'SP_STRKETIA':{1:'Yes',
                            2:'No'}},
                            inplace=True)
    return dfhuh
    






######################################################################################

def make_3d_plot(dfbene2008,title='Beneficiary file'):
    """make a 3d plot of lat/lng/elevation triples, 
    color-coded by State."""

    data = []
    cluster = []

    for i in range(len(dfbene2008['State'].unique())):
        name = dfbene2008['State'].unique()[i]
        x = dfbene2008[ dfbene2008['State'] == name ]['lng']
        y = dfbene2008[ dfbene2008['State'] == name ]['lat']
        z = dfbene2008[ dfbene2008['State'] == name ]['elevation']
    
        trace = dict(
            name = name,
            x = x, y = y, z = z,
            type = "scatter3d",    
            mode = 'markers',
            marker = dict( size=3, line=dict(width=0) ) )
        data.append( trace )

    layout = dict(
        width=800,
        height=550,
        autosize=False,
        title=title,
        scene=dict(
            xaxis=dict(
                gridcolor='rgb(255, 255, 255)',
                zerolinecolor='rgb(255, 255, 255)',
                showbackground=True,
                backgroundcolor='rgb(230, 230,230)'
            ),
            yaxis=dict(
                gridcolor='rgb(255, 255, 255)',
                zerolinecolor='rgb(255, 255, 255)',
                showbackground=True,
                backgroundcolor='rgb(230, 230,230)'
            ),
            zaxis=dict(
                gridcolor='rgb(255, 255, 255)',
                zerolinecolor='rgb(255, 255, 255)',
                showbackground=True,
                backgroundcolor='rgb(230, 230,230)'
            ),
            aspectratio = dict( x=1, y=1, z=0.7 ),
            aspectmode = 'manual'        
        ),
    )

    fig = dict(data=data,layout=layout)
    plotly.offline.iplot(fig, validate=False)



########################################################################################
    

def make_hcpcs_dataframe(filehcpcs='OPTUM_HCPCS_BASE_2016_01_update.TXT'):
    """Make the dataframe corresponding to the hcpcs codes."""
    dfhcpcs = pd.read_fwf(filehcpcs,skip_blank_lines=True,header=None,widths=[5,2084],
        names=['code','description'])
    return dfhcpcs

###########################################################################################


def make_cpc_dataframe(filecpc = 'OPTUM_CPC_BASE_2016_01.TXT'):
    """Make the dataframe corresponding to the cpc (cpt) codes."""
    dfcpc = pd.read_fwf(filecpc,header=None,skip_blank_lines=True,
                     widths=[5,10000],names=['code','description'])
    return dfcpc


########################################################################################


def make_code_dataframe():
    """Combine the level1 and level2 code information into a single dataframe."""
    df1 = make_hcpcs_dataframe()
    df2 = make_cpc_dataframe()
    dftotal = df1[['code','description']].append(df2[['code','description']])
    return dftotal





###########################################################################

def make_clean_carrier(url):
    """Make a clean dataframe corresponding to a carrier claim file.

    if url is not corresponding to a carrier file, raise an error
    and exit. Otherwise return a dataframe with dates properly 
    transformed and the geographical enhanced."""
    import datetime as dt

    if 'Carrier' not in url:
        print('That url does not correspond to a carrier file!')
        return None

    dfraw = process_url(url)
    dfcodes = make_code_dataframe()

    dficd9 = make_icd9_dataframe()

    dficd9dx = make_icd9dx_dataframe()

    dficd9sg = make_icd9sg_dataframe()

    
    
    for s in list(dfraw.columns)[4:12]:
        icd9dx_description_trans(dfraw, dficd9dx,s)


        
    for s in list(dfraw.columns)[38:51]:
        hcpcs_description_trans(dfraw,dfcodes,s)
    
    dfraw['CLM_FROM_DT'] = pd.to_datetime(dfraw['CLM_FROM_DT'].astype(str), format='%Y%m%d')

    dfraw['CLM_THRU_DT'] = pd.to_datetime(dfraw['CLM_THRU_DT'].astype(str), format='%Y%m%d')


    for s in list(dfraw.columns)[116:129]:
        line_prcsg_ind_cd_trans(dfraw,s)

    for s in list(dfraw.columns)[129:142]:
        icd9dx_description_trans(dfraw, dficd9dx, s)
    
    
    return dfraw


###########################################################################


def make_icd9_dataframe():
    """Make a dataframe that includes the icd9 code descriptions."""
    dficd9sg = pd.read_excel('CMS32_DESC_LONG_SHORT_SG.xlsx',
                        convert_float=False,
                        converters = {'PROCEDURE CODE': str})
    dficd9sg = dficd9sg.rename(columns={'PROCEDURE CODE': 'code',
                        'LONG DESCRIPTION': 'long_description',
                        'SHORT DESCRIPTION': 'short_description'})
    dficd9dx = pd.read_excel('CMS32_DESC_LONG_SHORT_DX.xlsx',
                        convert_float=False,
                        converters = {'PROCEDURE CODE': str})
    dficd9dx = dficd9dx.rename(columns={'DIAGNOSIS CODE': 'code',
                                    'LONG DESCRIPTION': 'long_description',
                        'SHORT DESCRIPTION': 'short_description'})
    dficd9 = dficd9dx.append(dficd9sg)
    return dficd9

###########################################################################

def make_icd9dx_dataframe():
    """Make a dataframe that includes the icd9dx descriptions."""
    
    dficd9dx = pd.read_excel('CMS32_DESC_LONG_SHORT_DX.xlsx',
                        convert_float=False,
                        converters = {'PROCEDURE CODE': str})
    dficd9dx = dficd9dx.rename(columns={'DIAGNOSIS CODE': 'code',
                                    'LONG DESCRIPTION': 'long_description',
                        'SHORT DESCRIPTION': 'short_description'})

    return dficd9dx


############################################################################

def make_icd9sg_dataframe():
    """Make a dataframe that includes the icd9sg descriptins."""
    
    dficd9sg = pd.read_excel('CMS32_DESC_LONG_SHORT_SG.xlsx',
                        convert_float=False,
                        converters = {'PROCEDURE CODE': str})
    dficd9sg = dficd9sg.rename(columns={'PROCEDURE CODE': 'code',
                        'LONG DESCRIPTION': 'long_description',
                        'SHORT DESCRIPTION': 'short_description'})

    return dficd9sg

###########################################################################

def make_drg_dataframe():
    """Make a dataframe that includes the MS-DRG code descriptions."""
    dfdrg = pd.read_excel('FY 2010 FR Table 5.xls')

    return dfdrg

############################################################################

def drg_description_trans(df,dfdrg,columnname):
    """takes the dataframe df and the columnname as a string and adds 
    a column containing the description to df using the
    description string in dfdrg."""

    dfhuh = pd.merge(df[[columnname]], dfdrg,
                     left_on=columnname, right_on='MS-DRG ',how='left')
    df[columnname + '_description'] = dfhuh['MS-DRG Title']
    del dfhuh



###########################################################################

def icd9_description_trans(df,dficd9,columnname):
    """takes the dataframe df and the columnname as a string and adds a 
    column containing the long description to df using the 
    description string in dficd9dx"""
    
    dfhuh = pd.merge(df[[columnname]], dficd9,
                 left_on=columnname, right_on='code',how='left')
    df[columnname + '_description'] = dfhuh['long_description']
    del dfhuh



##############################################################################

def icd9dx_description_trans(df,dficd9dx,columnname):
    """takes the dataframe df and the columnname as a string and adds a 
    column containing the long description to df using the 
    description string in dficd9dx"""
    
    dfhuh = pd.merge(df[[columnname]], dficd9dx,
                 left_on=columnname, right_on='code',how='left')
    df[columnname + '_description'] = dfhuh['long_description']
    del dfhuh



###############################################################################

def icd9sg_description_trans(df,dficd9sg,columnname):
    """takes the dataframe df and the columnname as a string and adds a 
    column containing the long description to df using the 
    description string in dficd9sg"""
    
    dfhuh = pd.merge(df[[columnname]], dficd9sg,
                 left_on=columnname, right_on='code',how='left')
    df[columnname + '_description'] = dfhuh['long_description']
    del dfhuh
    
    
################################################################################

    
    
def hcpcs_description_trans(df,dfhcpcs12,columnname):
    """Takes the dataframe df and the columnname as a string and adds
    a column containing the long description to df using the description string
    in dfhcpcs12."""
    dfhuh = pd.merge(df[[columnname]], dfhcpcs12,
                    left_on=columnname, right_on='code',how='left')
    df[columnname + '_description'] = dfhuh['description']
    del dfhuh


####################################################################################



def line_prcsg_ind_cd_trans(df,columnname):
    """Takes the dataframe with the given column name 
    and performs the necessary replacements as given 
    in https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public
    -Use-Files/SynPUFs/Downloads/SynPUF_Codebook.pdf"""
    df.replace({columnname:{'A': 'Allowed',
                            'B': 'Benefits exhausted',
                            'C': 'Noncovered care',
                            'D': 'Denied (existed prior to 1991; from BMAD)',
                            'I': 'Invalid data',
                            'L': 'CLIA (eff 9/92)',
                            'M': 'Multiple submittal - duplicate line item',
                            'N': 'Medically unnecessary',
                            'O': 'Other',
                            'P': 'Physician ownership of denial',
                            'Q': 'MSP cost avoided (contractor #88888) - voluntary agreement (eff. 1/98)',
                            'R': 'Reprocessed - adjustments based on subsequent reprocessing of claim',
                            'S': 'Secondary payer',
                            'T': 'MSP cost avoided - IEQ contractor (eff. 7/76)',
                            'U': 'MSP cost avoided - HMO rate cell adjustment (eff. 7/96)',
                            'V': 'MSP cost avoided - litigation settlement (eff. 7/96)',
                            'X': 'MSP cost avoided - generic',
                            'Y': 'MSP cost avoided - IRS/SSA data matach project',
                            'Z': 'Bundled test, no payment (eff. 1/1/98)',
                            '00': 'MSP cost avoided - COB Contractor',
                            '12': 'MSP cost avoided - BC/BS Voluntary Agreements',
                            '13': 'MSP cost avoided - Office of Personnel Management',
                            '14': "MSP cost avoided - Workman's Compensation (WC) Datamatch",
     '15': "MSP cost avoided - Workman's Compensation Insurer Voluntary Data Sharing Agreements (WC VDSA) (eff. 4/2006)",
     '16': "MSP cost avoided - Liability Insurer VDSA (eff.4/2006)",
    '17': "MSP cost avoided - No-Fault Insurer VDSA  (eff.4/2006)",
    '18': "MSP cost avoided - Pharmacy Benefit Manager Data Sharing Agreement (eff.4/2006)",
    '21': "MSP cost avoided - MIR Group Health Plan (eff.1/2009)",
     '22': "MSP cost avoided - MIR non-Group Health Plan (eff.1/2009)",
    '25': "MSP cost avoided - Recovery Audit Contractor - California (eff.10/2005)",
    '26': "MSP cost avoided - Recovery Audit Contractor - Florida (eff.10/2005)",
    '!': "MSP cost avoided - COB Contractor ('00' 2-byte code)",
    '@': "MSP cost avoided - BC/BS Voluntary Agreements ('12' 2-byte code)",
    '#': "MSP cost avoided - Office of Personnel Management ('13' 2-byte code)",
    '$': "MSP cost avoided - Workman's Compensation (WC) Datamatch ('14' 2-byte code)",
    '*': "MSP cost avoided - Workman's Compensation Insurer Voluntary Data Sharing Agreements (WC VDSA) ('15' 2-byte code) (eff. 4/2006)",
    '(':"MSP cost avoided - Liability Insurer VDSA ('16' 2-byte code) (eff. 4/2006)",
    ')':"MSP cost avoided - No-Fault Insurer VDSA ('17' 2-byte code) (eff. 4/2006)",
    '+':"MSP cost avoided - Pharmacy Benefit Manager Data Sharing Agreement ('18' 2 -byte code) (eff. 4/2006)",
    '<':"MSP cost avoided - MIR Group Health Plan ('21' 2-byte code) (eff. 1/2009)",
    '>':"MSP cost avoided - MIR non-Group Health Plan ('22' 2-byte code) (eff. 1/2009)",
    '%':"MSP cost avoided - Recovery Audit Contractor - California ('25' 2-byte code) (eff. 10/2005)",
    '&':"MSP cost avoided - Recovery Audit Contractor - Florida ('26' 2-byte code) (eff. 10/2005)"
                            }}, inplace=True)

###########################################################################################

def make_clean_inpatient(url):
    """Make a clean dataframe corresponding to aa inpatient claims file.

    if url is not corresponding to an inpatient claims  file, raise an error
    and exit. Otherwise return a dataframe with dates properly 
    transformed and the geographical enhanced."""
    import datetime as dt

    if 'Inpatient' not in url:
        print('That url does not correspond to an inpatient file!')
        return None

    dfraw = process_url(url)
    dfcodes = make_code_dataframe()

    dficd9 = make_icd9_dataframe()

    dficd9dx = make_icd9dx_dataframe()

    dficd9sg = make_icd9sg_dataframe()

    dfdrg = make_drg_dataframe()

    huh = dfraw['CLM_FROM_DT'][dfraw['SEGMENT'] != 2].astype(int).astype(str)
    duh = pd.to_datetime(huh, format='%Y%m%d')
    dfraw['CLM_FROM_DT'] = duh

    huh = dfraw['CLM_THRU_DT'][dfraw['SEGMENT'] != 2].astype(int).astype(str)
    duh = pd.to_datetime(huh, format='%Y%m%d')
    dfraw['CLM_THRU_DT'] = duh
        

    dfraw['CLM_ADMSN_DT'] = pd.to_datetime(dfraw['CLM_ADMSN_DT'].astype(str), format='%Y%m%d')

    for s in list(dfraw.columns)[12:13]:
        icd9dx_description_trans(dfraw, dficd9dx, s)

    dfraw['NCH_BENE_DSCHRG_DT'] = pd.to_datetime(dfraw['NCH_BENE_DSCHRG_DT'].astype(str), format='%Y%m%d')


    for s in list(dfraw.columns)[19:20]:
        drg_description_trans(dfraw, dfdrg, s)

    for s in list(dfraw.columns)[20:30]:
        icd9dx_description_trans(dfraw, dficd9dx, s)

    for s in list(dfraw.columns)[30:36]:
        try:
            huh = dfraw[s][dfraw[s].notnull()].astype(int).astype(str)
        except:
            huh = dfraw[s][dfraw[s].notnull()].astype(str)
        dfraw[s] = huh
        del huh

    for s in list(dfraw.columns)[30:36]:
        icd9sg_description_trans(dfraw, dficd9sg, s)


    for s in list(dfraw.columns)[36:81]:
        hcpcs_description_trans(dfraw,dfcodes,s)

    return dfraw

########################################################################################################################

def make_clean_outpatient(url):
    """Make a clean dataframe corresponding to aa outpatient claims file.

    if url is not corresponding to an outpatient claims  file, raise an error
    and exit. Otherwise return a dataframe with dates properly 
    transformed and the geographical enhanced."""
    import datetime as dt

    if 'Outpatient' not in url:
        print('That url does not correspond to an outpatient file!')
        return None

    dfraw = process_url(url)
    dfcodes = make_code_dataframe()

    dficd9 = make_icd9_dataframe()

    dficd9dx = make_icd9dx_dataframe()

    dficd9sg = make_icd9sg_dataframe()

    dfdrg = make_drg_dataframe()

    huh = dfraw['CLM_FROM_DT'][dfraw['SEGMENT'] != 2].astype(int).astype(str)
    duh = pd.to_datetime(huh, format='%Y%m%d')
    dfraw['CLM_FROM_DT'] = duh

    huh = dfraw['CLM_THRU_DT'][dfraw['SEGMENT'] != 2].astype(int).astype(str)
    duh = pd.to_datetime(huh, format='%Y%m%d')
    dfraw['CLM_THRU_DT'] = duh

    for s in list(dfraw.columns)[12:22]:
        icd9dx_description_trans(dfraw, dficd9dx, s)


    for s in list(dfraw.columns)[22:28]:
        try:
            huh = dfraw[s][dfraw[s].notnull()].astype(int).astype(str)
        except:
            huh = dfraw[s][dfraw[s].notnull()].astype(str)
        dfraw[s] = huh
        del huh

        
    for s in list(dfraw.columns)[22:28]:
        icd9sg_description_trans(dfraw, dficd9sg, s)

    for s in list(dfraw.columns)[30:31]:
        icd9dx_description_trans(dfraw, dficd9dx, s)


    for s in list(dfraw.columns)[31:76]:
        hcpcs_description_trans(dfraw,dfcodes,s)
    
    return dfraw

##########################################################################################################################

def make_clean_drugs(url):
    """Make a clean dataframe corresponding to a Prescription Drug Events file.

    if url is not corresponding to a drugs file, raise an error
    and exit. Otherwise return a dataframe with dates properly 
    transformed and the geographical enhanced."""
    import datetime as dt

    if 'Drug' not in url:
        print('That url does not correspond to an Drug file!')
        return None

    dfraw = process_url(url)

    dfraw['SRVC_DT'] = pd.to_datetime(dfraw['SRVC_DT'].astype(str), format='%Y%m%d')

    return dfraw

################################################################################################################################
