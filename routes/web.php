<?php

use App\Http\Controllers\ProfileController;
use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| Web Routes
|--------------------------------------------------------------------------
|
| Here is where you can register web routes for your application. These
| routes are loaded by the RouteServiceProvider and all of them will
| be assigned to the "web" middleware group. Make something great!
|
*/

Route::get('/', function () {
    return view('welcome');
});

Route::get('/dashboard', function () {
    return redirect()->route('dashboard.utama');
})->middleware(['auth', 'verified'])->name('dashboard');

Route::middleware('auth')->group(function () {
    Route::post('/dashboard', [ProfileController::class, 'update'])->name('dashboard.update');
    Route::get('/profile', [ProfileController::class, 'edit'])->name('profile.edit');
    Route::patch('/profile', [ProfileController::class, 'update'])->name('profile.update');
    Route::delete('/profile', [ProfileController::class, 'destroy'])->name('profile.destroy');

    // Routes from struktur_menu.json
    Route::get('/dashboard/utama', function() { return view('placeholder', ['pageTitle' => 'Dashboard Utama']); })->name('dashboard.utama');
    Route::get('/dashboard/video-report', function() { return view('placeholder', ['pageTitle' => 'Dashboard Pelaporan Video']); })->name('dashboard.video-report');
    Route::get('/analitik-transaksi/master_impor', function() { return view('placeholder', ['pageTitle' => 'Impor Data Transaksi (CSV)']); })->name('analitik-transaksi.master_impor');
    Route::get('/analitik-transaksi/input-pat', function() { return view('placeholder', ['pageTitle' => 'Input PAT Manual']); })->name('analitik-transaksi.input-pat');
    Route::get('/analitik-transaksi/filter-dtpa', function() { return view('placeholder', ['pageTitle' => 'Manajemen Filter DTPA']); })->name('analitik-transaksi.filter-dtpa');
    Route::get('/analitik-transaksi/data-dtpa', function() { return view('placeholder', ['pageTitle' => 'Tabel Data DTPA']); })->name('analitik-transaksi.data-dtpa');
    Route::get('/analitik-video/video_analitik', function() { return view('placeholder', ['pageTitle' => 'Analisis Waktu Kejadian Baru']); })->name('analitik-video.video_analitik');
    Route::get('/analitik-video/plat_nomor_analitik', function() { return view('placeholder', ['pageTitle' => 'Analis Plat Nomor Baru (LicenseFlat)']); })->name('analitik-video.plat_nomor_analitik');
    Route::get('/analitik-video/input-pav', function() { return view('placeholder', ['pageTitle' => 'Input PAV Manual']); })->name('analitik-video.input-pav');
    Route::get('/analitik-video/data-dpava', function() { return view('placeholder', ['pageTitle' => 'Tabel Data DPAVA']); })->name('analitik-video.data-dpava');
    Route::get('/validasi/mapping-dppt', function() { return view('placeholder', ['pageTitle' => 'Mapping Pelanggaran (DPPT)']); })->name('validasi.mapping-dppt');
    Route::get('/validasi/user-validation', function() { return view('placeholder', ['pageTitle' => 'Validasi Final User']); })->name('validasi.user-validation');
    Route::get('/laporan/dppt', function() { return view('placeholder', ['pageTitle' => 'Laporan DPPT (Dokumen)']); })->name('laporan.dppt');
    Route::get('/master/mor', function() { return view('placeholder', ['pageTitle' => 'Data MOR']); })->name('master.mor');
    Route::get('/master/provinsi', function() { return view('placeholder', ['pageTitle' => 'Data Provinsi']); })->name('master.provinsi');
    Route::get('/master/kabupaten-kota', function() { return view('placeholder', ['pageTitle' => 'Data Kabupaten/Kota']); })->name('master.kabupaten-kota');
    Route::get('/master/spbu', function() { return view('placeholder', ['pageTitle' => 'Data SPBU']); })->name('master.spbu');
    Route::get('/admin/users', function() { return view('placeholder', ['pageTitle' => 'Manajemen Pengguna & Role']); })->name('admin.users');
    Route::get('/admin/settings', function() { return view('placeholder', ['pageTitle' => 'Konfigurasi Umum']); })->name('admin.settings');
    Route::get('/help/glossary', function() { return view('help.glossary'); })->name('help.glossary');
    Route::get('/teknologi', function() { return view('help.teknologi'); })->name('teknologi');
    Route::get('/help/manual', function() { return view('help.manual'); })->name('help.manual');
    Route::get('/help/faq', function() { return view('help.faq'); })->name('help.faq');
    Route::get('/help/support', function() { return view('help.support'); })->name('help.support');
});

require __DIR__.'/auth.php';
