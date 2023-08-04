# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 20:50:31 2023

@author: sasre
"""

class WebpageData:
  def __init__(self, last_updated, price, n_beds, n_baths, avbl_date, 
               building_address, location, city):
    self.last_updated = last_updated
    self.price = price
    self.n_beds = n_beds
    self.n_baths = n_baths
    self.avbl_date = avbl_date
    self.building_address = building_address
    self.location = location
    self.city = city


