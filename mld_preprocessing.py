#!/usr/bin/env python
# coding: utf-8

import os
import numpy as np
from tools.pre_function import getll_nc,getdata_nc,interp2d_array,write_nc,interp2d_nc

### interpolate MLD into rough grids from 20190101 to 20190103

### interpolate days separately and combine with nco

# Basic settings
orgn_file = 'HYCOM_inv_20190101-0103.nc' # original data file name
lon,lat = getll_nc(orgn_file)            # get original lon and lat
lon_sel = lon[lon>120]                   # set lon range

for date in {'01','02','03'}:              # date info: from 0101 to 0103
    # Step1: read MLD from the original file
    var_list = getdata_nc(                 # get variable data
        orgn_file,                         # original file name
        ['MLD_Tdiff_est','MLD_Tdiff_dia'], # variable name (list)
        lon_sel=lon_sel,                   # select lon
        timestr='2019-01-'+date            # select time
    ) 
    var1, var2 = var_list[0], var_list[1]  # separate variables
    mask = np.isnan(var1)                  # save land mask
    var1[mask]=0                           # set land point to 0
    var2[mask]=0
    
    
    # Step2: interpolate data into rough grids
    var1_int = interp2d_array(    # interpolate variable 1
        lon_sel, lat, var1,       # original grids
        lon_sel[::10], lat[::10], # new grids: 1/10 lower resolution
        method='linear'           # select interpolation method
    )
    var2_int = interp2d_array(    # interpolate variable 2
        lon_sel, lat, var2,       
        lon_sel[::10], lat[::10], 
        method='linear'           
    )
    
    var1_int[mask[::10,::10]] = np.nan # mask land
    var2_int[mask[::10,::10]] = np.nan

    var1_int[var1_int>500]=500           # quality control for MLD
    var2_int[var2_int>500]=500
    var1_int[var1_int<0]=0
    var2_int[var2_int<0]=0
    
    
    # Step3: write interpolated results into a new netcdf file
    rslt_file = 'HYCOM_inv_201901'+date+'_int.nc' # output file name
    write_nc(                                                                                      # write interpolated file
        rslt_file,                                                                                 # output file name
        [lon_sel[::10],lat[::10],np.expand_dims(var1_int,axis=0),np.expand_dims(var2_int,axis=0)], # variable data
        ['longitude','latitude','MLD_Tdiff_est','MLD_Tdiff_dia'],                                  # variable names 
        ['longitude','latitude',('time','latitude','lontitude'),('time','latitude','lontitude')],  # variable dimensions
        '2019-01-'+date,                                                                           # time
        cdl_file='nwp_mld.cdl' # set cdl file name for generating netcdf file using ncgen if no empty file exists
    )

    
# Step4: combine files into one file for 20190101-20190103 using nco
os.system('ncrcat -O HYCOM_inv_20190101_int.nc HYCOM_inv_20190102_int.nc HYCOM_inv_20190103_int.nc HYCOM_inv_20190101-0103_int_eg1.nc')



### generate landmask file from one day's interpolated results and interpolate all days together

# Step1: prepare mask file
lon,lat = getll_nc(rslt_file)    # get interpolated lon and lat
int_data = getdata_nc(           # get interpolated variable data
    'HYCOM_inv_20190101_int.nc', # interpolated file from the block above
    'MLD_Tdiff_est',             # variable name
)[0]                             # select array from list

mask = np.int64(int_data==0)  # generate mask array, 1 for mased points

write_nc(                                                     # write mask file
    'nwp_mask.nc',                                            # mask file name
    [lon,lat,np.tile(mask,(3,1,1))], 
    ['longitude','latitude','LANDMASK'],
    ['longitude','latitude',('time','latitude','lontitude')],
    ['2019-01-01','2019-01-02','2019-01-03'],                 # time list
    cdl_file='nwp_mask.cdl' # set cdl file name for generating netcdf file using ncgen if no empty file exists
)


# Step2: interpolate and write results into a new netcdf file
rslt_file = 'HYCOM_inv_20190101-0103_int_eg2.nc'
interp2d_nc(                                      # interpolate and write file
    orgn_nc=orgn_file,                            # original data file name
    var_names=['MLD_Tdiff_est','MLD_Tdiff_dia'],  # interpolated variable list
    rslt_nc=rslt_file,                            # output file name
    mask_nc='nwp_mask.nc'                         # mask file name
)


# Step3: quality control
orgn_list = getdata_nc(                # get interpolated variable data
    rslt_file,                         # interpolated file name
    ['MLD_Tdiff_est','MLD_Tdiff_dia'],
)

var1,var2 = orgn_list[0],orgn_list[1]  # separate variables
var1[var1>500]=500                     # quality control for MLD
var2[var2>500]=500
var1[var1<0]=0
var2[var2<0]=0


# Step4: write results into the existed file
write_nc(
    rslt_file,
    [lon,lat,var1,var2],
    ['longitude','latitude','MLD_Tdiff_est','MLD_Tdiff_dia'],
    ['longitude','latitude',('time','latitude','lontitude'),('time','latitude','lontitude')],
    ['2019-01-01','2019-01-02','2019-01-03'], # time list                                                                         # time
)




