import unittest
import sys
import types

sys.modules.setdefault("pandas", types.SimpleNamespace(DataFrame=object, to_datetime=None))
sys.modules.setdefault(
    "polars",
    types.SimpleNamespace(
        Date=object,
        Datetime=object,
        DataFrame=object,
        Float32=object,
        Int32=object,
        String=object,
    ),
)
sys.modules.setdefault("sodapy", types.SimpleNamespace(Socrata=object))

from ulod.base import Source
from ulod.ckan import Canada, CKAN, Madrid, SessionCKAN, StreamResponse
from ulod.ckan.client import CKAN as CKANClient
from ulod.ckan.portals import Madrid as MadridPortal
from ulod.ods import Bologna, ODS, Paris
from ulod.ods.client import ODS as ODSClient
from ulod.ods.portals import Paris as ParisPortal
from ulod.socrata import Chicago, NYC, SocrataClient, cast_socrata_types
from ulod.socrata.client import SocrataClient as SocrataClientBase
from ulod.socrata.portals import NYC as NYCPortal
from ulod.socrata.utils import cast_socrata_types as cast_socrata_types_from_utils


class SourceTypeImportTests(unittest.TestCase):
    def test_source_type_packages_export_expected_classes(self):
        self.assertTrue(issubclass(CKAN, Source))
        self.assertTrue(issubclass(SessionCKAN, CKAN))
        self.assertTrue(issubclass(Canada, CKAN))
        self.assertTrue(issubclass(Madrid, SessionCKAN))
        self.assertEqual(StreamResponse.__name__, "StreamResponse")

        self.assertTrue(issubclass(ODS, Source))
        self.assertTrue(issubclass(Bologna, ODS))
        self.assertTrue(issubclass(Paris, ODS))

        self.assertTrue(issubclass(SocrataClient, Source))
        self.assertTrue(issubclass(Chicago, SocrataClient))
        self.assertTrue(issubclass(NYC, SocrataClient))
        self.assertTrue(callable(cast_socrata_types))

    def test_implementation_modules_export_expected_classes(self):
        self.assertIs(CKANClient, CKAN)
        self.assertIs(MadridPortal, Madrid)
        self.assertIs(ODSClient, ODS)
        self.assertIs(ParisPortal, Paris)
        self.assertIs(SocrataClientBase, SocrataClient)
        self.assertIs(NYCPortal, NYC)
        self.assertIs(cast_socrata_types_from_utils, cast_socrata_types)


if __name__ == "__main__":
    unittest.main()
