###############################################################
# Copyright 2014 Lawrence Livermore National Security, LLC
# (c.f. AUTHORS, NOTICE.LLNS, COPYING)
#
# This file is part of the Flux resource manager framework.
# For details, see https://github.com/flux-framework.
#
# SPDX-License-Identifier: LGPL-3.0
###############################################################

import json

from flux.wrapper import Wrapper
from flux.future import Future
from flux.core.inner import ffi, raw
import flux.constants
from flux.util import encode_payload, encode_topic


class RPC(Future):
    """A class whose instances represent remote procedure calls (rpc). 

    Instances of this class hold state relating to a single rpc call.
    They can be waited on, 
    """

    def __init__(
        self,
        flux_handle,
        topic,
        payload=None,
        nodeid=flux.constants.FLUX_NODEID_ANY,
        flags=0,
    ):
        """Construct an RPC object.

        Submits a message with given payload to a Flux instance.
        
        :param flux_handle: a Flux object, representing the actual 
        Flux program to submit the message to
        :param topic: a string indicating the type of method call
        :param payload: the (optional) payload to include in the method call
        :type payload: None, str, bytes, unicode, or json-serializable
        """
        if isinstance(flux_handle, Wrapper):
            # keep the flux_handle alive for the lifetime of the RPC
            self.flux_handle = flux_handle
            flux_handle = flux_handle.handle

        topic = encode_topic(topic)
        payload = encode_payload(payload)

        future_handle = raw.flux_rpc(flux_handle, topic, payload, nodeid, flags)
        super(RPC, self).__init__(future_handle, prefixes=["flux_rpc_", "flux_future_"])

    def get_str(self):
        """Block and then return the response message of this RPC object.

        This method returns a JSON str, or None.
        """ 
        payload_str = ffi.new("char *[1]")
        self.pimpl.flux_rpc_get(payload_str)
        if payload_str[0] == ffi.NULL:
            return None
        return ffi.string(payload_str[0]).decode("utf-8")

    def get(self):
        """Block and then return the response data of this RPC object.

        Unlike the RPC.get_str method, this method decodes the JSON 
        response str, and returns a list, dict, or None.
        """
        resp_str = self.get_str()
        if resp_str is None:
            return None
        return json.loads(resp_str)
