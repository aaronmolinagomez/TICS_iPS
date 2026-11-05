@file:OptIn(ExperimentalFoundationApi::class)

package com.example.beaconips

import android.Manifest
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.os.Build
import android.os.Bundle
import android.os.ParcelUuid
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.pager.HorizontalPager
import androidx.compose.foundation.pager.rememberPagerState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.content.ContextCompat
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewmodel.compose.viewModel
import com.example.beaconips.ui.PixelLightning
import com.example.beaconips.ui.pixels.LightningPixels
import com.example.beaconips.ui.theme.BeaconIPSTheme
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient
import com.hivemq.client.mqtt.datatypes.MqttQos
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.util.*
import kotlin.math.*

// ================= VIEWMODEL =================
class BeaconViewModel : ViewModel() {
    private val _isBeaconActive = MutableStateFlow(false)
    val isBeaconActive: StateFlow<Boolean> = _isBeaconActive.asStateFlow()
    fun setBeaconActive(active: Boolean) { _isBeaconActive.value = active }
}

// ================= MAIN ACTIVITY =================
class MainActivity : ComponentActivity() {

    companion object {
        private const val TAG = "BEACON_IPS"
        val BEACON_UUID: UUID = UUID.fromString("12345678-1234-1234-1234-1234567890AB")
        val ANCHOR_IPS = listOf("192.168.1.41","192.168.1.42","192.168.1.43","192.168.1.44")
        const val MQTT_BROKER_HOST = "192.168.1.5"
        const val MQTT_BROKER_PORT = 1883
        const val MQTT_TOPIC = "ips/mobile"
        private const val SEND_INTERVAL_MS = 100L // Enviar cada 100ms
    }

    // ================= BLE =================
    private lateinit var bluetoothAdapter: BluetoothAdapter
    private var advertiser: BluetoothLeAdvertiser? = null
    private val isAdvertising = mutableStateOf(false)

    private val advertiseCallback = object : AdvertiseCallback() {
        override fun onStartSuccess(settingsInEffect: AdvertiseSettings) {
            Log.d(TAG, "BLE advertise START success")
            isAdvertising.value = true
        }
        override fun onStartFailure(errorCode: Int) {
            Log.e(TAG, "BLE advertise START failed: $errorCode")
            isAdvertising.value = false
        }
    }

    // ================= COROUTINES =================
    private val ioScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // ================= NETWORK =================
    private var mqttClient: Mqtt5AsyncClient? = null
    @Volatile private var isMqttConnected = false
    @Volatile private var isActive = false

    // ================= SENSORS =================
    private lateinit var sensorManager: SensorManager
    private var accelerometer: Sensor? = null
    private var rotationVector: Sensor? = null
    private var stepDetector: Sensor? = null

    @Volatile private var accelX = 0f
    @Volatile private var accelY = 0f
    @Volatile private var accelZ = 0f

    // Renombrado: estos NO son giroscopio, son ORIENTACIÓN (ángulos de Euler)
    @Volatile private var azimuthDegrees = 0f      // Rotación respecto al Norte (0-360°)
    @Volatile private var pitchDegrees = 0f        // Inclinación vertical (-180 a 180°)
    @Volatile private var rollDegrees = 0f         // Inclinación lateral (-90 a 90°)

    @Volatile private var stepCounter = 0
    @Volatile private var lastStepValue = 0

    private val sensorListener = object : SensorEventListener {
        override fun onSensorChanged(event: SensorEvent) {
            when(event.sensor.type) {
                Sensor.TYPE_ACCELEROMETER -> {
                    if (!event.values.all { it == 0f }) {
                        accelX = event.values[0]
                        accelY = event.values[1]
                        accelZ = event.values[2]
                    }
                }
                Sensor.TYPE_ROTATION_VECTOR -> {
                    if (!event.values.all { it == 0f }) {
                        val rotationMatrix = FloatArray(9)
                        SensorManager.getRotationMatrixFromVector(rotationMatrix, event.values)
                        val orientationAngles = FloatArray(3)
                        SensorManager.getOrientation(rotationMatrix, orientationAngles)

                        azimuthDegrees = Math.toDegrees(orientationAngles[0].toDouble()).toFloat()
                        pitchDegrees = Math.toDegrees(orientationAngles[1].toDouble()).toFloat()
                        rollDegrees = Math.toDegrees(orientationAngles[2].toDouble()).toFloat()

                        azimuthDegrees = (azimuthDegrees + 360) % 360
                    }
                }
                Sensor.TYPE_STEP_DETECTOR -> {
                    stepCounter++
                }
            }
        }
        override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) {}
    }

    // ================= PERMISSIONS =================
    @Volatile private var permissionsGranted = false
    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { permissions ->
        val fineLocation = permissions[Manifest.permission.ACCESS_FINE_LOCATION] ?: false
        val activityRecognition = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q)
            permissions[Manifest.permission.ACTIVITY_RECOGNITION] ?: false
        else true
        val blePermissions = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S)
            (permissions[Manifest.permission.BLUETOOTH_ADVERTISE] ?: false) &&
                    (permissions[Manifest.permission.BLUETOOTH_CONNECT] ?: false)
        else
            permissions[Manifest.permission.BLUETOOTH] ?: false
        permissionsGranted = fineLocation && activityRecognition && blePermissions
        if (permissionsGranted) Log.d(TAG, "✅ Permisos concedidos") else Log.e(TAG,"❌ Permisos denegados")
    }

    // ================= BLUETOOTH ENABLE LAUNCHER =================
    private val enableBluetoothLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { _ ->
        if (bluetoothAdapter.isEnabled) continueStartBeacon() else isActive = false
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val manager = getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
        bluetoothAdapter = manager.adapter
        advertiser = bluetoothAdapter.bluetoothLeAdvertiser

        requestPermissions()

        sensorManager = getSystemService(Context.SENSOR_SERVICE) as SensorManager
        accelerometer = sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER)
        rotationVector = sensorManager.getDefaultSensor(Sensor.TYPE_ROTATION_VECTOR)
        stepDetector = sensorManager.getDefaultSensor(Sensor.TYPE_STEP_DETECTOR)

        setContent {
            BeaconIPSTheme {
                Surface(modifier = Modifier.fillMaxSize(), color = Color.Black) {
                    val viewModel: BeaconViewModel = viewModel()
                    MainScreen(
                        viewModel = viewModel,
                        onStartBeacon = { startBeacon() },
                        onStopBeacon = { stopBeacon() }
                    )
                }
            }
        }
    }

    private fun requestPermissions() {
        val permissionsToRequest = mutableListOf(
            Manifest.permission.INTERNET,
            Manifest.permission.ACCESS_FINE_LOCATION
        )
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q)
            permissionsToRequest.add(Manifest.permission.ACTIVITY_RECOGNITION)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            permissionsToRequest.add(Manifest.permission.BLUETOOTH_ADVERTISE)
            permissionsToRequest.add(Manifest.permission.BLUETOOTH_CONNECT)
            permissionsToRequest.add(Manifest.permission.BLUETOOTH_SCAN)
        } else {
            permissionsToRequest.add(Manifest.permission.BLUETOOTH)
            permissionsToRequest.add(Manifest.permission.BLUETOOTH_ADMIN)
        }
        permissionLauncher.launch(permissionsToRequest.toTypedArray())
    }

    private fun checkPermissions(): Boolean {
        if (!permissionsGranted) return false
        val fineLocation = ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
        val blePermission = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_ADVERTISE) == PackageManager.PERMISSION_GRANTED &&
                    ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED
        } else ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH) == PackageManager.PERMISSION_GRANTED
        val activityRecognition = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q)
            ContextCompat.checkSelfPermission(this, Manifest.permission.ACTIVITY_RECOGNITION) == PackageManager.PERMISSION_GRANTED
        else true
        return fineLocation && blePermission && activityRecognition
    }

    // ================= BEACON CONTROL =================
    private fun startBeacon() {
        if (!checkPermissions()) { Log.e(TAG,"Permisos insuficientes"); return }
        if (isActive) return
        if (!bluetoothAdapter.isEnabled) {
            enableBluetoothLauncher.launch(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE))
            return
        }
        continueStartBeacon()
    }

    private fun continueStartBeacon() {
        if (isActive) return
        isActive = true
        try {
            advertiser?.startAdvertising(
                AdvertiseSettings.Builder()
                    .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_LOW_LATENCY)
                    .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_HIGH)
                    .setConnectable(false)
                    .build(),
                AdvertiseData.Builder()
                    .setIncludeTxPowerLevel(true)
                    .addServiceUuid(ParcelUuid(BEACON_UUID))
                    .build(),
                advertiseCallback
            )
            startTracking()
        } catch (e: Exception) {
            Log.e(TAG,"Error continueStartBeacon: ${e.message}")
            isActive = false
        }
    }

    private fun stopBeacon() {
        isActive = false
        try { advertiser?.stopAdvertising(advertiseCallback) } catch (_: Exception) {}
        isAdvertising.value = false
        stopTracking()
    }

    // ================= TRACKING =================
    private fun startTracking() {
        connectMqtt()
        startContinuousSending()

        try {
            accelerometer?.let {
                sensorManager.registerListener(sensorListener, it, SensorManager.SENSOR_DELAY_GAME)
            }
            rotationVector?.let {
                sensorManager.registerListener(sensorListener, it, SensorManager.SENSOR_DELAY_GAME)
            }
            stepDetector?.let {
                sensorManager.registerListener(sensorListener, it, SensorManager.SENSOR_DELAY_GAME)
            }
        } catch (_: Exception) {}
    }

    private fun stopTracking() {
        sensorManager.unregisterListener(sensorListener)
        try { mqttClient?.disconnect() } catch (_: Exception) {}
        mqttClient = null
        isMqttConnected = false
    }

    // ================= MQTT =================
    private fun connectMqtt() {
        try {
            mqttClient = MqttClient.builder()
                .useMqttVersion5()
                .identifier("android_${System.currentTimeMillis()}")
                .serverHost(MQTT_BROKER_HOST)
                .serverPort(MQTT_BROKER_PORT)
                .buildAsync()

            mqttClient?.connectWith()?.send()?.whenComplete { _, throwable ->
                isMqttConnected = throwable == null
                Log.d(TAG,"MQTT connected: $isMqttConnected")
            }
        } catch (_: Exception) { isMqttConnected = false }
    }

    private fun startContinuousSending() {
        ioScope.launch {
            while (isActive) {
                sendImuData()
                delay(SEND_INTERVAL_MS)
            }
        }
    }

    private fun sendImuData() {
        if (!isActive) return
        val client = mqttClient ?: return
        if (!isMqttConnected) return

        ioScope.launch {
            try {
                val stepsInInterval = stepCounter - lastStepValue
                lastStepValue = stepCounter

                val payload = JSONObject().apply {
                    put("accel_x", accelX)
                    put("accel_y", accelY)
                    put("accel_z", accelZ)

                    // AHORA CON NOMBRES CORRECTOS Y EN GRADOS
                    put("azimuth", azimuthDegrees)      // 0-360° (0 = Norte)
                    put("pitch", pitchDegrees)          // Inclinación vertical
                    put("roll", rollDegrees)            // Inclinación lateral

                    put("step", stepsInInterval)
                    put("timestamp", System.currentTimeMillis())
                }

                client.publishWith()
                    .topic(MQTT_TOPIC)
                    .payload(payload.toString().toByteArray())
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send()
            } catch (_: Exception) {}
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        stopBeacon()
        ioScope.cancel()
    }
}

// ================= UI =================

// (Mantengo TODO UI igual, no se toca nada)
// ================= UI =================

@Composable
fun MainScreen(
    viewModel: BeaconViewModel,
    onStartBeacon: () -> Unit,
    onStopBeacon: () -> Unit
) {
    val pagerState = rememberPagerState(pageCount = { 4 })
    val logs = remember { mutableStateListOf<String>() }
    val isBeaconActive by viewModel.isBeaconActive.collectAsState()

    Column(modifier = Modifier.fillMaxSize()) {
        HorizontalPager(state = pagerState, modifier = Modifier.weight(1f)) { page ->
            when (page) {
                0 -> BeaconPage(
                    isActive = isBeaconActive,
                    onToggle = {
                        if (!isBeaconActive) {
                            viewModel.setBeaconActive(true)
                            onStartBeacon()
                        } else {
                            viewModel.setBeaconActive(false)
                            onStopBeacon()
                        }
                    }
                )
                1 -> AnchorsPage(logs)
                2 -> LogsPage(logs)
                3 -> SimplePage("PAGE FOUR")
            }
        }

        Spacer(Modifier.height(12.dp))
        DotsIndicator(4, pagerState.currentPage)
        Spacer(Modifier.height(16.dp))
    }
}

@Composable
fun BeaconPage(
    isActive: Boolean,
    onToggle: () -> Unit
) {
    Column(
        modifier = Modifier.fillMaxSize(),
        verticalArrangement = Arrangement.Center,
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        DiamondButton(isActive) {
            onToggle()
        }

        Text(
            text = if (isActive) "BEACON ACTIVO" else "BEACON INACTIVO",
            color = if (isActive) Color.Green else Color.Red,
            fontSize = 16.sp,
            modifier = Modifier.padding(top = 20.dp)
        )
    }
}

@Composable
fun AnchorsPage(logs: MutableList<String>) {
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    val anchorIps = MainActivity.ANCHOR_IPS

    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(24.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        Text("ESP32 CONTROL", color = Color.White, fontSize = 20.sp)

        anchorIps.forEach { ip ->
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text(ip, color = Color.LightGray, fontSize = 14.sp)

                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    SimpleButton("ON") {
                        scope.launch {
                            sendScan(ip, "on", logs)
                        }
                    }
                    SimpleButton("OFF") {
                        scope.launch {
                            sendScan(ip, "off", logs)
                        }
                    }
                }
            }
        }

        Spacer(Modifier.height(24.dp))
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceEvenly
        ) {
            SimpleButton("ALL ON") {
                scope.launch {
                    anchorIps.forEach {
                        launch { sendScan(it, "on", logs) }
                    }
                }
            }
            SimpleButton("ALL OFF") {
                scope.launch {
                    anchorIps.forEach {
                        launch { sendScan(it, "off", logs) }
                    }
                }
            }
        }
    }
}

@Composable
fun SimpleButton(text: String, onClick: () -> Unit) {
    Box(
        modifier = Modifier
            .clip(RoundedCornerShape(6.dp))
            .background(Color.DarkGray)
            .clickable { onClick() }
            .padding(horizontal = 20.dp, vertical = 10.dp),
        contentAlignment = Alignment.Center
    ) {
        Text(text, color = Color.White, fontSize = 14.sp)
    }
}

suspend fun sendScan(
    ip: String,
    value: String,
    logs: MutableList<String>
) = withContext(Dispatchers.IO) {
    val urlStr = "http://$ip/?scan=$value"
    val time = System.currentTimeMillis()

    suspend fun log(msg: String) {
        Log.d("NET", msg)
        withContext(Dispatchers.Main) {
            if (logs.size > 100) logs.removeAt(logs.lastIndex)
            logs.add(0, "[$time] $msg")
        }
    }

    log("📡 REQUEST: $urlStr")

    try {
        val url = URL(urlStr)
        val conn = url.openConnection() as HttpURLConnection
        conn.requestMethod = "GET"
        conn.connectTimeout = 3000
        conn.readTimeout = 3000
        conn.connect()

        val code = conn.responseCode
        log("✅ RESPONSE CODE: $code")

        val reader = if (code in 200..299)
            BufferedReader(InputStreamReader(conn.inputStream))
        else
            BufferedReader(InputStreamReader(conn.errorStream))

        val body = reader.readText()
        reader.close()
        if (body.isNotBlank()) log("📦 BODY: ${body.take(100)}")
        conn.disconnect()
    } catch (e: Exception) {
        log("❌ ERROR: ${e.message ?: "Unknown"}")
    }
}

@Composable
fun LogsPage(logs: List<String>) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(16.dp)
            .verticalScroll(rememberScrollState())
    ) {
        Text("NETWORK LOGS", color = Color.White, fontSize = 18.sp)
        Spacer(Modifier.height(8.dp))

        if (logs.isEmpty()) {
            Text("No logs yet", color = Color.Gray, fontSize = 14.sp)
        } else {
            logs.take(50).forEach { log ->
                Text(
                    log,
                    color = if (log.contains("✅")) Color.Green else Color.LightGray,
                    fontSize = 11.sp,
                    modifier = Modifier.padding(vertical = 2.dp)
                )
            }
        }
    }
}

@Composable
fun SimplePage(text: String) {
    Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
        Text(text, color = Color.Gray, fontSize = 24.sp)
    }
}

@Composable
fun DotsIndicator(total: Int, selected: Int) {
    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.Center) {
        repeat(total) {
            Box(
                modifier = Modifier
                    .padding(4.dp)
                    .size(if (it == selected) 8.dp else 6.dp)
                    .clip(CircleShape)
                    .background(if (it == selected) Color.White else Color.DarkGray)
            )
        }
    }
}

@Composable
fun DiamondButton(active: Boolean, onToggle: () -> Unit) {
    Box(
        modifier = Modifier
            .size(140.dp)
            .graphicsLayer { rotationZ = 45f }
            .clip(RoundedCornerShape(14.dp))
            .background(if (active) Color.White else Color.DarkGray)
            .pointerInput(Unit) { detectTapGestures { onToggle() } },
        contentAlignment = Alignment.Center
    ) {
        Column(
            modifier = Modifier.graphicsLayer { rotationZ = -45f },
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            PixelLightning(Modifier.size(60.dp), LightningPixels, Color.Black)
            Text(
                if (active) "ACTIVE" else "INACTIVE",
                fontWeight = FontWeight.Bold,
                color = if (active) Color.Black else Color.White,
                fontSize = 14.sp
            )
        }
    }
}