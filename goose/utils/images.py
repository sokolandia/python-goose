# -*- coding: utf-8 -*-
"""\
This is a python port of "Goose" orignialy licensed to Gravity.com
under one or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.

Python port was written by Xavier Grangier for Recrutae

Gravity.com licenses this file
to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import hashlib
import io
import functools
from PIL import Image
from goose.utils.encoding import smart_str
from goose.image import ImageDetails
from goose.image import LocallyStoredImage

from google.appengine.api import urlfetch, memcache


def get_image_hash(src):
    return hashlib.md5(smart_str(src)).hexdigest()


class ImageUtils(object):

    @classmethod
    def get_image_dimensions(self, entity):
        image_details = ImageDetails()
        try:
            image = Image.open(io.BytesIO(entity))
            image_details.set_mime_type(image.format)
            width, height = image.size
            image_details.set_width(width)
            image_details.set_height(height)
        except IOError:
            image_details.set_mime_type('NA')
        return image_details

    @classmethod
    def store_image(self, http_client, link_hash, src, config):
        """\
        Writes an image src http string to disk as a temporary file
        and returns the LocallyStoredImage object
        that has the info you should need on the image
        """
        # check for a cache hit already on disk
        src_hash = get_image_hash(src)
        image = memcache.get(src_hash)
        if image:
            return image

        # no cache found download the image
        data = self.fetch(http_client, src)
        if data:
            image = self.write_localfile(data, link_hash, src_hash, src, config)
            if image:
                memcache.set(key=image.local_filename, value=image, time=86400)
                return image

        return None

    @classmethod
    def store_images(self, link_hash, src_list, config):
        src_hashes = map(get_image_hash, src_list)
        images = memcache.get_multi(src_hashes)
        result = [None] * len(src_list)
        rpcs = []
        cache = {}

        def handle_result(rpc, index):
            try:
                req = rpc.get_result()
                if req.status_code == 200:
                    image = self.write_localfile(req.content, link_hash, src_hashes[index], src_list[index], config)
                    result[index] = cache[image.local_filename] = image
            except urlfetch.Error:
                pass

        for i, src_hash in enumerate(src_hashes):
            image = images.get(src_hash)
            if not image:
                rpc = urlfetch.create_rpc()
                rpc.callback = functools.partial(handle_result, rpc, i)
                urlfetch.make_fetch_call(rpc, src_list[i])
                rpcs.append(rpc)
            else:
                result[i] = image
        for rpc in rpcs:
            rpc.wait()
        if cache:
            memcache.add_multi(cache)
        return filter(lambda x: x, result)

    @classmethod
    def get_mime_type(self, image_details):
        mime_type = image_details.get_mime_type().lower()
        mimes = {
            'png': '.png',
            'jpg': '.jpg',
            'jpeg': '.jpg',
            'gif': '.gif',
        }
        return mimes.get(mime_type, 'NA')

    @classmethod
    def write_localfile(self, entity, link_hash, src_hash, src, config):
        image_details = self.get_image_dimensions(entity)
        return LocallyStoredImage(
            src=src,
            local_filename=src_hash,
            link_hash=link_hash,
            bytes=len(entity),
            file_extension=self.get_mime_type(image_details),
            height=image_details.get_height(),
            width=image_details.get_width()
        )

    @classmethod
    def clean_src_string(self, src):
        return src.replace(" ", "%20")

    @classmethod
    def fetch(self, http_client, src):
        try:
            req = urlfetch.fetch(src)
            if req.status_code == 200:
                return req.content
            else:
                return None
        except urlfetch.Error:
            return None
