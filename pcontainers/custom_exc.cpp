#include <new>
#include <typeinfo>
#include <stdexcept>
#include <ios>
#include "Python.h"
#include "_py_exceptions.h"
#include "lmdb_exceptions.h"
#include "custom_exc.h"

namespace quiet {

using namespace lmdb;

void custom_exception_handler() {
    // Catch a handful of different errors here and turn them into the equivalent Python errors.
    try {
        if (PyErr_Occurred()) {
            ; // let the latest Python exn pass through and ignore the current one
        }
        else {
            throw;
        }

    } catch (const not_initialized& exn) {
        PyErr_SetString(py_not_initialized, exn.what());

    } catch (const empty_key& exn) {
        PyErr_SetString(py_empty_key, exn.what());

    } catch (const empty_database& exn) {
        PyErr_SetString(py_empty_database, exn.what());

    } catch (const io_error& exn) {
        PyErr_SetString(PyExc_IOError, exn.what());

    } catch (const system_error& exn) {
        PyErr_SetString(PyExc_IOError, exn.what());

    } catch (const access_error& exn) {
        PyErr_SetString(py_access_error, exn.what());

    } catch (const mdb_keyexist& exn) {
        PyErr_SetString(py_key_exist, exn.what());

    } catch (const mdb_page_notfound& exn) {
        PyErr_SetString(py_page_not_found, exn.what());

    } catch (const key_not_found& exn) {
        PyErr_SetString(py_key_not_found, exn.what());

    } catch (const mdb_notfound& exn) {
        PyErr_SetString(py_not_found, exn.what());

    } catch (const mdb_currupted& exn) {
        PyErr_SetString(py_corrupted, exn.what());

    } catch (const mdb_panic& exn) {
        PyErr_SetString(py_panic, exn.what());

    } catch (const mdb_version_mismatch& exn) {
        PyErr_SetString(py_version_mismatch, exn.what());

    } catch (const mdb_invalid& exn) {
        PyErr_SetString(py_invalid, exn.what());

    } catch (const mdb_map_full& exn) {
        PyErr_SetString(py_map_full, exn.what());

    } catch (const mdb_dbs_full& exn) {
        PyErr_SetString(py_dbs_full, exn.what());

    } catch (const mdb_readers_full& exn) {
        PyErr_SetString(py_readers_full, exn.what());

    } catch (const mdb_tls_full& exn) {
        PyErr_SetString(py_tls_full, exn.what());

    } catch (const mdb_txn_full& exn) {
        PyErr_SetString(py_txn_full, exn.what());

    } catch (const mdb_cursor_full& exn) {
        PyErr_SetString(py_cursor_full, exn.what());

    } catch (const mdb_page_full& exn) {
        PyErr_SetString(py_page_full, exn.what());

    } catch (const mdb_map_resized& exn) {
        PyErr_SetString(py_map_resized, exn.what());

    } catch (const mdb_incompatible& exn) {
        PyErr_SetString(py_incompatible, exn.what());

    } catch (const mdb_bad_rslot& exn) {
        PyErr_SetString(py_bad_rslot, exn.what());

    } catch (const mdb_bad_txn& exn) {
        PyErr_SetString(py_bad_txn, exn.what());

    } catch (const mdb_bad_valsize& exn) {
        PyErr_SetString(py_bad_val_size, exn.what());

    } catch (const mdb_bad_dbi& exn) {
        PyErr_SetString(py_bad_dbi, exn.what());

    } catch (const lmdb_error& exn) {
        PyErr_SetString(py_lmdb_error, exn.what());



    } catch (const std::bad_alloc& exn) {
        PyErr_SetString(PyExc_MemoryError, exn.what());
    } catch (const std::bad_cast& exn) {
        PyErr_SetString(PyExc_TypeError, exn.what());
    } catch (const std::domain_error& exn) {
        PyErr_SetString(PyExc_ValueError, exn.what());
    } catch (const std::invalid_argument& exn) {
        PyErr_SetString(PyExc_ValueError, exn.what());
    } catch (const std::ios_base::failure& exn) {
        // Unfortunately, in standard C++ we have no way of distinguishing EOF
        // from other errors here; be careful with the exception mask
        PyErr_SetString(PyExc_IOError, exn.what());
    } catch (const std::out_of_range& exn) {
        // Change out_of_range to IndexError
        PyErr_SetString(PyExc_IndexError, exn.what());
    } catch (const std::overflow_error& exn) {
        PyErr_SetString(PyExc_OverflowError, exn.what());
    } catch (const std::range_error& exn) {
        PyErr_SetString(PyExc_ArithmeticError, exn.what());
    } catch (const std::underflow_error& exn) {
        PyErr_SetString(PyExc_ArithmeticError, exn.what());
    } catch (const std::exception& exn) {
        PyErr_SetString(PyExc_RuntimeError, exn.what());
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
    }
}

}
