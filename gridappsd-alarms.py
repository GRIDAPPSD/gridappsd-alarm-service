# -------------------------------------------------------------------------------
# Copyright (c) 2017, Battelle Memorial Institute All rights reserved.
# Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity
# lawfully obtaining a copy of this software and associated documentation files (hereinafter the
# Software) to redistribute and use the Software in source and binary forms, with or without modification.
# Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and may permit others to do so, subject to the following conditions:
# Redistributions of source code must retain the above copyright notice, this list of conditions and the
# following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
# the following disclaimer in the documentation and/or other materials provided with the distribution.
# Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any
# form whatsoever without the express written consent of Battelle.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
# General disclaimer for use with OSS licenses
#
# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
# -------------------------------------------------------------------------------
"""
Created on Jan 19, 2018

@author: Poorva Sharma
"""

import argparse
import json
import logging
import math
import sys
import time

from gridappsd import GridAPPSD, DifferenceBuilder, utils, topics
from gridappsd.topics import simulation_input_topic, simulation_log_topic, simulation_output_topic

class SimulationSubscriber(object):
    """ A simple class that handles publishing forward and reverse differences

    The object should be used as a callback from a GridAPPSD object so that the
    on_message function will get called each time a message from the simulator.  During
    the execution of on_meessage the `CapacitorToggler` object will publish a
    message to the simulation_input_topic with the forward and reverse difference specified.
    """

    def __init__(self, simulation_id, gridappsd_obj, eq_name_map, eq_meas_map, meas_eq_map):
        """ Create a ``SimulationSubscriber`` object

        This object is used as a subscription callback from a ``GridAPPSD``
        object.  This class receives simulation output and publishes 
	voltage violation with the frequency as defined by message_period.

        Parameters
        ----------
        simulation_id: str
            The simulation_id to use for publishing to a topic.
        gridappsd_obj: GridAPPSD
            An instatiated object that is connected to the gridappsd message bus
            usually this should be the same object which subscribes, but that
            isn't required.
        capacitor_dict: list(str)
            A list of capacitors mrids to turn on/off
        """
        self._gapps = gridappsd_obj
	self.eq_name_map = eq_name_map
        self.eq_meas_map = eq_meas_map
        self.meas_eq_map = meas_eq_map
        self.meas_input_map = {}
        self.rcvd_input_list = {}
        self.previous_value = {}
        self._publish_to_topic = topics.service_output_topic('gridappsd-alarms',simulation_id)

    def on_message(self, headers, message):
        """ Handle incoming messages on the simulation_output_topic for the simulation_id

        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """
        alarms = []
        if "input" in headers["destination"]:
            for item in message["input"]['message']['forward_differences']:
                if item["object"] in self.eq_name_map.keys():
                    if item["attribute"] == "ShuntCompensator.sections" or item["attribute"] == "Switch.open":
                        rcvd_input = {}
                        rcvd_input["equipment_mrid"] = item["object"]
                        rcvd_input["value"] = item["value"]
                        rcvd_input["created_by"] = headers["GOSS_SUBJECT"]
                        rcvd_input["equipment_name"] = self.capacitors_dict[item["object"]]["IdentifiedObject.name"]
                        self.rcvd_input_list[eq_meas_map[item["object"]]] = rcvd_input
        
        if "output" in headers[destination]:
            for meas_mrid in self.rcvd_input_list:
                output_value = message["message"]["measurements"][meas_mrid]["value"]
                if output_value != self.previous_value[meas_mrid] and output_value == self.rcvd_input_list[meas_mrid]["value"]:
                    alarms.append(self.rcvd_input_list[meas_mrid])
                    self.rcvd_input_list.remove[meas_mrid]
            for meas_mrid in self.meas_eq_map.keys():
                self.previous_value[meas_mrid] = message["message"]["measurements"][meas_mrid]["value"]

 
        if alarms:         
	    self._gapps.send(self._publish_to_topic, json.dumps(alarms_list))

def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    # These are now set through the docker container interface via env variables or defaulted to
    # proper values.
    #
    # parser.add_argument("-u", "--user", default="system",
    #                     help="The username to authenticate with the message bus.")
    # parser.add_argument("-p", "--password", default="manager",
    #                     help="The password to authenticate with the message bus.")
    # parser.add_argument("-a", "--address", default="127.0.0.1",
    #                     help="tcp address of the mesage bus.")
    # parser.add_argument("--port", default=61613, type=int,
    #                     help="the stomp port on the message bus.")
    #
    opts = parser.parse_args()
    sim_output_topic = simulation_output_topic(opts.simulation_id)
    sim_input_topic = simulation_input_topic(opts.simulation_id)
    sim_request = json.loads(opts.request.replace("\'",""))
    model_mrid = sim_request["power_system_config"]["Line_name"]
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
    eq_meas_map = {}
    eq_name_map = {}
    meas_eq_map = {} 

    request = {
        "modelId": model_mrid,
        "requestType": "QUERY_OBJECT_DICT",
        "resultFormat": "JSON",
        "objectType": "LinearShuntCompensator"
        }

    response = gapps.get_response("goss.gridappsd.process.request.data.powergridmodel",request)
    for capacitor in response["data"]:
        eq_name_map[capacitor["id"]] = capacitor["IdentifiedObject.name"]

    request = {
        "modelId": model_mrid,
        "requestType": "QUERY_OBJECT_DICT",
        "resultFormat": "JSON",
        "objectType": "LoadBreakSwitch"
        }

    response = gapps.get_response("goss.gridappsd.process.request.data.powergridmodel",request)
    for switch in response["data"]:
        eq_name_map[switch["id"]] = switch["IdentifiedObject.name"]

    request = {"modelId": model_mrid,
               "requestType": "QUERY_OBJECT_MEASUREMENTS",
               "resultFormat": "JSON",
               "objectType": "LinearShuntCompensator"
               }
    
    response = gapps.get_response("goss.gridappsd.process.request.data.powergridmodel",request)
    for measurement in response["data"]:
        if measurement["type"] == "Pos":
            eq_meas_map[measurement["eqid"]] = measurement["measid"]
            meas_eq_map[measurement["measid"]] = measurement["eqid"]
        
    request = {"modelId": model_mrid,
               "requestType": "QUERY_OBJECT_MEASUREMENTS",
               "resultFormat": "JSON",
               "objectType": "LoadBreakSwitch"
               }
    
    response = gapps.get_response("goss.gridappsd.process.request.data.powergridmodel",request)
    for measurement in response["data"]:
        if measurement["type"] == "Pos":
            eq_meas_map[measurement["eqid"]] = measurement["measid"]
            meas_eq_map[measurement["measid"]] = measurement["eqid"]


    subscriber = SimulationSubscriber(opts.simulation_id, gapps, eq_name_map, eq_meas_map, meas_eq_map)
    gapps.subscribe(sim_input_topic, subscriber)
    gapps.subscribe(sim_output_topic, subscriber)
    while True:
        time.sleep(0.1)


if __name__ == "__main__":
    _main()
