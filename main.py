import dandi.dandiarchive as da
from src.Pipeline import Pipeline, PipelineJob, PipelineJobInput, PipelineJobOutput, PipelineJobRequiredResources, PipelineImportedFile
from src.utils import _remote_file_exists
import numpy as np


num_assets_to_process = 1


def main():
    #  Large-scale recordings of head-direction cells in mouse postsubiculum
    dandiset_id = '000871'
    dandiset_version = 'draft'
    dendro_project_id = '2d2b13fd'  # https://dendro.vercel.app/project/2d2b13fd?tab=project-home
    # project: D-000871
    pipeline = Pipeline(project_id=dendro_project_id)

    # First example: https://neurosift.app/?p=/nwb&dandisetId=000871&dandisetVersion=draft&url=https://api.dandiarchive.org/api/assets/e0acd690-970a-4f37-bf38-a99e169bc773/download/

    parsed_url = da.parse_dandi_url(f"https://dandiarchive.org/dandiset/{dandiset_id}")

    # # this is one where I can see the cells
    # asset_id = 'd06f9c5e-2540-439f-8998-eeb9f9e4e63b'
    # asset_path = 'sub-644972/sub-644972_ses-1238929581-acq-1239101619-raw-movies_image+ophys.nwb'
    # lindi_url = f'https://lindi.neurosift.org/dandi/dandisets/{dandiset_id}/assets/{asset_id}/zarr.json'
    # test_process_asset(lindi_url)
    # return

    with parsed_url.navigate() as (client, dandiset, assets):
        if dandiset is None:
            print(f"Dandiset {dandiset_id} not found.")
            return

        num_consecutive_not_nwb = 0
        num_consecutive_not_found = 0
        num_assets_processed = 0
        for asset_obj in dandiset.get_assets('path'):
            if not asset_obj.path.endswith(".nwb"):
                num_consecutive_not_nwb += 1
                if num_consecutive_not_nwb >= 20:
                    # For example, this is important for 000026 because there are so many non-nwb assets
                    print("Stopping dandiset because too many consecutive non-NWB files.")
                    break
                continue
            else:
                num_consecutive_not_nwb = 0
            print(asset_obj.path)
            print('=====================')
            if num_consecutive_not_found >= 20:
                print("Stopping dandiset because too many consecutive missing files.")
                break
            if num_assets_processed >= 100:
                print("Stopping dandiset because 100 assets have been processed.")
                break
            asset_id = asset_obj.identifier
            asset_path = asset_obj.path
            lindi_url = f'https://lindi.neurosift.org/dandi/dandisets/{dandiset_id}/assets/{asset_id}/zarr.json'
            if not _remote_file_exists(lindi_url):
                num_consecutive_not_found += 1
                continue
            print(f'Processing file {asset_path} ({num_assets_processed})')
            name = f'{dandiset_id}/{asset_path}.lindi.json'

            pipeline.add_imported_file(PipelineImportedFile(
                fname=f'imported/{name}',
                url=lindi_url,
                metadata={
                    'dandisetId': dandiset_id,
                    'dandisetVersion': dandiset_version,
                    'dandiAssetId': asset_id
                }
            ))
            create_compressed_videos(
                pipeline=pipeline,
                input=f'imported/{name}',
                output=f'generated/{name}/compressed_videos.nwb.lindi.json',
                metadata={
                    'dandisetId': dandiset_id,
                    'dandisetVersion': dandiset_version,
                    'dandiAssetId': asset_id,
                    'supplemental': True
                }
            )

            num_assets_processed += 1
            if num_assets_processed >= num_assets_to_process:
                break
    print('Submitting pipeline')
    pipeline.submit()


# def test_process_asset(lindi_url: str):
#     import lindi
#     import h5py
#     from neurosift.codecs import MP4AVCCodec
#     MP4AVCCodec.register_codec()
#     with lindi.StagingArea.create('staging') as staging_area:
#         x = lindi.LindiH5pyFile.from_reference_file_system(lindi_url, mode='r+', staging_area=staging_area)
#         G = x['acquisition/raw_suite2p_motion_corrected']
#         assert isinstance(G, h5py.Group)
#         data = G['data']
#         assert isinstance(data, h5py.Dataset)
#         conversion = data.attrs['conversion']
#         resolution = data.attrs['resolution']
#         starting_time_dataset = G['starting_time']
#         assert isinstance(starting_time_dataset, h5py.Dataset)
#         starting_time = starting_time_dataset[()]
#         rate = starting_time_dataset.attrs['rate']
#         for k, v in data.attrs.items():
#             print(f'{k}: {v}')
#         first_chunk = data[0:50]
#         assert isinstance(first_chunk, np.ndarray)
#         print(first_chunk.shape)
#         max_val = np.max(first_chunk)
#         normalization_factor = 255 / max_val
#         num_timepoints_per_chunk = 500
#         chunk_size = [num_timepoints_per_chunk, data.shape[1], data.shape[2]]
#         G2 = x['acquisition'].create_group('raw_suite2p_motion_corrected_compressed')
#         for k, v in G.attrs.items():
#             if k != 'object_id':
#                 G2.attrs[k] = v
#         import uuid
#         G2.attrs['object_id'] = str(uuid.uuid4())
#         for k in G.keys():
#             if k != 'data':
#                 x.copy('acquisition/raw_suite2p_motion_corrected/' + k, x, 'acquisition/raw_suite2p_motion_corrected_compressed/' + k)
#         codec = MP4AVCCodec(fps=rate)
#         G2.create_dataset_with_zarr_compressor('data', shape=data.shape, chunks=chunk_size, dtype=np.uint8, compressor=codec)
#         import time
#         timer = 0
#         for i in range(0, data.shape[0], num_timepoints_per_chunk):
#             elapsed = time.time() - timer
#             if elapsed > 3:
#                 pct_complete = i / data.shape[0]
#                 timer = time.time()
#                 print(f'{pct_complete * 100:.1f}% complete')
#             chunk = data[i:i + num_timepoints_per_chunk]
#             G2['data'][i:i + num_timepoints_per_chunk] = (chunk * normalization_factor).astype(np.uint8)
#         # A = (first_chunk * normalization_factor).astype(np.uint8)
#         # codec = MP4AVCCodec(fps=rate)
#         # buf = codec.encode(A)
#         # with open('test.mp4', 'wb') as f:
#         #     f.write(buf)

def create_compressed_videos(*, pipeline: Pipeline, input: str, output: str, metadata: dict):
    pipeline.add_job(PipelineJob(
        processor_name='neurosift-1.compressed_videos',
        inputs=[
            PipelineJobInput(name='input', fname=input)
        ],
        outputs=[
            PipelineJobOutput(name='output', fname=output, metadata=metadata)
        ],
        parameters=[],
        required_resources=PipelineJobRequiredResources(
            num_cpus=4,
            num_gpus=0,
            memory_gb=16,
            time_sec=60 * 60 * 24
        ),
        run_method='local'
    ))


if __name__ == '__main__':
    main()
