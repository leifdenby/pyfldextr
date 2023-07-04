from pathlib import Path

from .fafile_to_netcdf import generate_vfld_and_load_from_fa_file, setup_gl

if __name__ == "__main__":
    fp_hm_home = Path("/perm/nhae/hm_lib/PVPS_aerosols_10apr2015__base")
    fp_exp_root = "/lus/h2resw01/scratch/nhae/hm_home/PVPS_aerosols_10apr2015__base"
    fprel_fafile = "archive/2015/04/14/09/ICMSHHARM+0003"
    
    setup_gl(conda_prefix=None, fp_hm_home=fp_hm_home)

    ds_synop = generate_vfld_and_load_from_fa_file(
        fp_exp_root=fp_exp_root, fprel_fafile=fprel_fafile, fp_hm_home=fp_hm_home
    )
    
    fn_out = f"{ds_synop.vfld_filename}.nc"
    ds_synop.to_netcdf(fn_out)
    logger.info(f"Wrote synop VFLD data to {fn_out}")
