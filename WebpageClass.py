# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 20:50:31 2023

@author: sasre
"""

class WebpageData:
  def __init__(self, last_updated, price, n_beds, n_baths, avbl_date, 
               building_address, area, agent_name, agent_email, agent_phone, agent_fees):
    self.last_updated = last_updated
    self.price = price
    self.n_beds = n_beds
    self.n_baths = n_baths
    self.avbl_date = avbl_date
    self.building_address = building_address
    self.area = area
    self.agent_name = agent_name
    self.agent_email = agent_email
    self.agent_phone = agent_phone
    self.agent_fees = agent_fees


