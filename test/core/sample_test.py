import uuid
from SampleService.core.sample import Sample

def test_sample_build():
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = Sample(id_)

    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef') 

