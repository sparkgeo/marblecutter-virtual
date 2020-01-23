# coding=utf-8

import json
import logging
import math

from boto3 import client
from marblecutter import Bounds, get_resolution_in_meters, get_source, get_zoom
from marblecutter.catalogs import WGS84_CRS, Catalog
from marblecutter.utils import Source
from os.path import splitext
from rasterio import warp
from rasterio.enums import Resampling
from urllib.parse import urlparse

LOG = logging.getLogger(__name__)
s3 = client('s3')

class VirtualCatalog(Catalog):
    _rgb = None
    _nodata = None
    _linear_stretch = None
    _resample = None

    def __init__(self, uri, alg_name=None, rgb=None, nodata=None, linear_stretch=None, resample=None):
        self._uri = uri

        if alg_name:
            self._alg_name = alg_name

        if rgb:
            self._rgb = rgb

        if nodata:
            self._nodata = nodata

        if linear_stretch:
            self._linear_stretch = linear_stretch

        try:
            # test whether provided resampling method is valid
            Resampling[resample]
            self._resample = resample
        except KeyError:
            self._resample = None

        self._meta = {}

        parsed_uri = urlparse(uri, allow_fragments=False)
        bucket = parsed_uri.netloc.split(".")[0]
        key = parsed_uri.path.lstrip("/")
        meta_json_key = f"{splitext(key)[0]}.json"
        result = s3.get_object(Bucket=bucket, Key=meta_json_key) 
        meta_json = json.loads(result["Body"].read().decode())

        with get_source(self._uri) as src:
            self._bounds = warp.transform_bounds(src.crs, WGS84_CRS, *src.bounds)
            self._resolution = get_resolution_in_meters(
                Bounds(src.bounds, src.crs), (src.height, src.width)
            )
            approximate_zoom = get_zoom(max(self._resolution), op=math.ceil)

            global_min = src.get_tag_item("TIFFTAG_MINSAMPLEVALUE")
            global_max = src.get_tag_item("TIFFTAG_MAXSAMPLEVALUE")

            maxes = meta_json[self._alg_name]["PER98"]
            if len(maxes) == 1:
                maxes = maxes * 3
            
            mins = meta_json[self._alg_name]["PER02"]
            if len(mins) == 1:
                mins = mins * 3

            means = meta_json[self._alg_name]["MEAN"]
            if len(means) == 1:
                means = means * 3

            self._rgb = meta_json[self._alg_name].get("RENDER_ORDER")

            self._algorithm = meta_json[self._alg_name].get("ALGORITHM")

            for band in range(0, 3):
                self._meta["values"] = self._meta.get("values", {})
                self._meta["values"][band] = {}
                min_val = mins[band]
                max_val = maxes[band]
                mean_val = means[band]

                if min_val is not None:
                    self._meta["values"][band]["min"] = float(min_val)
                elif global_min is not None:
                    self._meta["values"][band]["min"] = float(global_min)

                if max_val is not None:
                    self._meta["values"][band]["max"] = float(max_val)
                elif global_max is not None:
                    self._meta["values"][band]["max"] = float(global_max)

                if mean_val is not None:
                    self._meta["values"][band]["mean"] = float(mean_val)

        self._center = [
            (self._bounds[0] + self.bounds[2]) / 2,
            (self._bounds[1] + self.bounds[3]) / 2,
            approximate_zoom - 3,
        ]
        self._maxzoom = approximate_zoom + 3
        self._minzoom = approximate_zoom - 10

    @property
    def uri(self):
        return self._uri

    def get_sources(self, bounds, resolution):
        recipes = {"imagery": True}

        if self._rgb is not None:
            recipes["rgb_bands"] = map(int, self._rgb.split(","))

        if self._nodata is not None:
            recipes["nodata"] = self._nodata

        if self._linear_stretch is not None:
            recipes["linear_stretch"] = "per_band"

        if self._resample is not None:
            recipes["resample"] = self._resample

        if self._algorithm is not None:
            recipes["algorithm"] = self._algorithm

        yield Source(
            url=self._uri,
            name=self._name,
            resolution=self._resolution,
            band_info={},
            meta=self._meta,
            recipes=recipes,
        )
