#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import numpy.ma as ma
import netCDF4
from netCDF4 import Dataset
import xarray as xr
import pandas as pd
from scipy import interpolate
import math 
from pathlib import Path

from parcels import FieldSet,NestedField, ParticleSet, JITParticle, ScipyParticle, AdvectionRK4,DiffusionUniformKh, Variable, Field,GeographicPolar,Geographic
from datetime import timedelta as timedelta
import datetime
from parcels.tools.converters import TimeConverter
import glob
from datetime import datetime
import datetime

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.colors import ListedColormap
from matplotlib.lines import Line2D
from copy import copy
from os.path import isfile
import pytz
from os import path

# In[ ]:


# Delete and Egg movement Kernels


# In[ ]:

# Delete Error
def DeleteErrorParticle(particle, fieldset, time):
    if particle.state == StatusCode.ErrorOutOfBounds:
        particle.delete()

# Delete Kernel
def DeleteParticle(particle, fieldset, time):
    print('deleted particle')
    particle.delete()


# In[ ]:


def EggHatchingMovement(particle, fieldset, time):
    eggdepth = 0.25 # egg depth in m
    eggtime=1*86400
    vertical_speed = 0.02  # sink and rise speed in m/s
    drifttime1 =  30* 86400  # time of deep drift in seconds


    if particle.cycle_phase == 0:
        # Phase 0:particle is an egg and has yet to hatch
        particle.age += particle.dt
        particle.depth=eggdepth 
        if particle.age >= eggtime:
            particle.cycle_phase = 1 # phase 1 is sinking after "hatching"
            
    elif particle.cycle_phase == 1:
        # Phase 1: Sinking with vertical_speed until depth is driftdepth1
        particle_ddepth += vertical_speed * particle.dt
        particle.age += particle.dt # added this in because the particles are sinking to various/deep depths and will need more time than other particles (this may get resolved by increasing the run time but better to do this I think)
        if particle.depth + particle_ddepth >= particle.driftlayer:
            particle_ddepth = particle.driftlayer - particle.depth
            particle.cycle_phase = 2 # phase 2 is drifting in the first 25% of PLD

    elif particle.cycle_phase == 2:
        # Phase 2: Drifting at larval depth
        particle.age += particle.dt
        particle.depth=particle.driftlayer 

        
def AgeDelete(particle, fieldset, time):
    if particle.age > (31*86400):
        #print("soy vieja")
        particle.delete()
    
# In[ ]:



# first round of spawning
#startDate = '1992-02-14' 
#endDate = '1992-06-30'

# second round of spawning
#startDate = '1992-06-14' 
#endDate = '1992-10-30'

# 1992 for testing purposes
# Define start and end dates
startDate = '1992-10-02'
endDate = '1993-02-28'

# Wake Hycom Files are labeled by their time since epoch
# getting the desired start/end dates in time since epoch
desired_startdate = datetime.datetime(1992, 10, 2, tzinfo=pytz.utc)  # Year, Month, Day
desired_enddate = datetime.datetime(1993, 3, 1, tzinfo=pytz.utc) 

startseconds_since_epoch = desired_startdate.timestamp()
endseconds_since_epoch = desired_enddate.timestamp()
startseconds_since_epoch = int(startseconds_since_epoch)
endseconds_since_epoch = int(endseconds_since_epoch)

print(startseconds_since_epoch)
print(endseconds_since_epoch)

interval = 10800 # 3 hr interval in seconds 
print("start loading in files")
# In[ ]:


#file = glob.glob("/home/esd_data/Hycom_Wake/HYCOM_Wake_*.nc")
# create list of file paths between the start and end time in seconds sincce epoch 
expected_files = [f"/home/esd_data/Hycom_Wake/HYCOM_Wake_{seconds}.nc" for seconds in range(startseconds_since_epoch, endseconds_since_epoch, interval)]
#print(expected_files[0:5])
# Filter the list to just the files that actually exist. (This is for when you remove NA files)
actual_files = [f for f in expected_files if isfile(f)]

ds1 = xr.open_mfdataset(actual_files)  # this puts the opendap data into a xarray dataset
#print(ds1)
myDat1 = ds1.sel(**{'TIME': slice(startDate,endDate)})

# In[ ]:


variables = {'U': 'WATER_U',
             'V': 'WATER_V'}
dimensions = {'lon': 'LONGITUDE4001_4563',
              'lat': 'LATITUDE1064_1376',
              'time': 'TIME',
              'depth': 'LEV1_20'}

fieldset = FieldSet.from_xarray_dataset(myDat1, variables, dimensions)


# In[ ]:


kh = 10.0   # This is the eddy diffusivity in m2/s
fieldset.add_constant_field('Kh_zonal', kh, mesh='spherical')
#zonal follows lat
fieldset.add_constant_field('Kh_meridional', kh, mesh='spherical') 


# In[ ]:


class DisplacementParticle(JITParticle):
    #dU = Variable('dU', to_write = False)
    #dV = Variable('dV', to_write = False)
    #d2s = Variable('d2s', initial=1e3, to_write = False)
    age = Variable('age', dtype=np.float32, initial= 0., to_write = False)
    cycle_phase=Variable('cycle_phase', dtype=np.float32, initial=0., to_write = False)
    releaseSite = Variable('releaseSite', dtype=np.int32, to_write = False)
    #distance = Variable('distance', dtype=np.int32, initial=0.) # not calculating distance for now but left this in
    #prev_lat = Variable('prev_lat', initial=0., to_write=False)  
    #prev_lon = Variable('prev_lon', initial=0., to_write=False)
    f = Variable('f', dtype=np.int32, to_write = False)
    driftlayer = Variable('driftlayer', dtype=np.int32, to_write = False)

# In[ ]:



# In[ ]:


source_loc = pd.read_csv('/home/gmukai/Desktop/Wake_Parcel_Files/Wakedeeper_Taongi_Bikar_releasesites.csv', header=None)
# Number of particle released per location
npart_perlayer = 1
npart = 10*npart_perlayer

# Release location from the file read in above
lon = np.repeat(source_loc[0], npart)
lat = np.repeat(source_loc[1],npart)
site = np.repeat(source_loc[2],npart)
# Start date for release. Since we are releasing every set number of days the repeatdt version was simplest
#start_date = 0
dlayer = [0.25]*(len(source_loc)*npart)
driftlayers = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]  # Define the layers
repeating_layers = driftlayers * (len(source_loc)*npart_perlayer)

repeatdt = timedelta(days=1)

print("called in release points")
# In[ ]:




# In[ ]:

print("create pset now")
pset = ParticleSet.from_list(fieldset=fieldset, pclass=DisplacementParticle,lon=lon,lat=lat,releaseSite=site,
                             depth=dlayer, repeatdt=repeatdt, driftlayer = repeating_layers)
print("create kernels")

kernels = [EggHatchingMovement, displace, AdvectionRK4, DiffusionUniformKh, set_displacement, AgeDelete, DeleteErrorParticle]

# In[ ]:


output_file = pset.ParticleFile(name="Wake_1992_n10_n1perlayer_kh10_Oct_Feb_bounce_Bumphead6_17.zarr", outputdt=timedelta(hours=0.5))


# In[ ]:
model_dt=timedelta(minutes=10)

run_days = 76
print("start execute")
pset.execute(kernels,
            runtime=timedelta(days=run_days),
            dt=model_dt, 
            output_file=output_file)
pset.repeatdt = None

pset.execute(kernels,
            runtime=timedelta(days=31+1),
            dt=model_dt, 
            output_file=output_file)

#data_xarray = xr.open_zarr("Wake_1992_n10_kh10_Oct_Feb_bounce5_2_2.zarr")
#data_xarray.to_netcdf("Wake_1992_n10_kh10_Oct_Feb_bounce5_2_2.nc")
