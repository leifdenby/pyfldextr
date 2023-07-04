import tempfile
import subprocess
from pathlib import Path
import os
import pyvfld
from loguru import logger


FLDEXTR_NAMELIST = """
&NAMFLD
 LALADIN=F,
 EXP="PVPS_aerosols_10apr2015__base                     ",
 NLEV_T=11         ,
 PRES_T=  1000.0000000000000     ,  925.00000000000000     ,  850.00000000000000     ,  700.00000000000000     ,  500.00000000000000     ,
   400.00000000000000     ,  300.00000000000000     ,  200.00000000000000     ,  150.00000000000000     ,  100.00000000000000     ,  50.000000000000000     ,
  4*0.0000000000000000       ,
 MODEL=3          ,
 IEXCLUDE=10         ,
 LAND_LIMIT= -1.0000000000000000     ,
 LMRGP=F,
 SCAN_RADIUS=5          ,
 POLSTER_PROJLAT=  60.000000000000000     ,
 LHCORR=F,
 SEARCH_RADIUS=0          ,
 FILE_FORMAT= "FA                  ",
 TOPLEV=1          ,
 IS_ANALYSIS=F,
 SCREEN_LVL_FILE=-1         ,
 LIGS= 2*F, 3*T,
 USE_UA=T,
 LPRINTRAD=T,
 /
"""

def setup_gl(conda_prefix, fp_hm_home):
    if "CONDA_PREFIX" not in os.environ:
        os.environ["CONDA_PREFIX"] = conda_prefix
        os.environ["PATH"] += f":{conda_prefix}/bin"

    if not "ECCODES_DEFINITION_PATH" in os.environ:
        eccodes_default_defns_path = subprocess.check_output(["codes_info", "-d"]).decode()
        os.environ[
            "ECCODES_DEFINITION_PATH"
        ] = f"{fp_hm_home}/util/gl/definitions:{eccodes_default_defns_path}"  

def _execute(cmd, cwd):
    # https://stackoverflow.com/a/4417735
    popen = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, universal_newlines=True, cwd=cwd
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()

    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


def call_fldextr(fp_fldextr, fp_cwd, print_output=True):
    output_lines = []
    try:
        for output in _execute(cmd=str(fp_fldextr), cwd=fp_cwd):
            if print_output:
                print((output.strip()))
            output_lines.append(output.strip())

    except subprocess.CalledProcessError as ex:
        return_code = ex.returncode
        error_extra = ""
        if -return_code == signal.SIGSEGV:
            error_extra = ", the utility segfaulted "

        raise Exception(
            "There was a problem when running fldextr "
            "(errno={}): {} {}".format(error_extra, return_code, ex)
        )

    return "\n".join(output_lines)


def generate_vfld_and_load_from_fa_file(fp_exp_root, fp_hm_home, fprel_fafile):
    fp_exp_root = Path(fp_exp_root)
    with tempfile.TemporaryDirectory() as fp_temp:
        fp_temp = Path(fp_temp)
        with open(fp_temp / "fldextr.dat", "w") as fh:
            fh.write(FLDEXTR_NAMELIST)

        for station_kind in ["synop", "temp"]:
            fn_in = f"all{station_kind}.list"
            fn_out = f"{station_kind}.list"
            (fp_temp / fn_out).symlink_to(fp_hm_home / "util/gl/scr" / fn_in)

        (fp_temp / "fort.10").symlink_to(fp_exp_root / fprel_fafile)
        
        output = call_fldextr(fp_fldextr=f"{fp_exp_root}/bin/fldextr", fp_cwd=fp_temp)
    
        last_line = output.splitlines()[-1]
        
        sentinel = "OUTPUT TO:"
        if last_line.startswith(sentinel):
            fp_output = fp_temp / last_line[len(sentinel):]
            
        df_synop, _ = pyvfld.read_vlfd(fp_output)
        ds_synop = pyvfld.to_dataset(df_synop)
        ds_synop.attrs["source_file"] = str(fp_exp_root / fprel_fafile)
        ds_synop.attrs["vfld_filename"] = fp_output.name
        return ds_synop
