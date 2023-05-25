<?php

namespace Integrations\Brasilia2;

use DateTimeImmutable;
use DateTimeInterface;
use Exception;
use GuzzleHttp\Utils;
use Integrations\Brasilia2\Cache\Token;
use Integrations\Exception\PartiallyBlockedSeatsException;
use Integrations\Exception\PartiallyConfirmedSeatsException;
use Integrations\Exception\PartiallyUnblockedSeatsException;
use Integrations\Exception\SkippableBusException;
use Integrations\Exception\VendorRequestException;
use Integrations\HasServicesInterface;
use Integrations\Helper;
use Integrations\IntegrationOptions;
use Integrations\SpecificIntegration;
use Integrations\Util;
use Pimple\Container;
use Redbus\Dto\Booking\ConfirmResult;
use Redbus\Dto\Booking\IdProofType;
use Redbus\Dto\Booking\Request\IasCancelBooking;
use Redbus\Dto\Booking\Request\IasConfirmBooking;
use Redbus\Dto\Booking\Request\IasExpireBooking;
use Redbus\Dto\Booking\Request\IasExtendBooking;
use Redbus\Dto\Booking\Request\IasTentativeBooking;
use Redbus\Dto\Inventory\Bus;
use Redbus\Dto\Inventory\SeatAvailabilityStatus;
use Redbus\Dto\Inventory\SeatLayoutWrapper;
use Redbus\Dto\Inventory\SeatReservationType;
use Redbus\Dto\Inventory\SeatType;
use Redbus\Dto\Inventory\VendorFare;
use Redbus\Dto\Inventory\VSeatStatus;
use Redbus\Service\SettingsBag;
use RuntimeException;
use Service\Log\Wrapper\GuzzleLogWrapper;
use Service\Mds\Models\VendorSDPair;
use Service\Mds\Models\VendorCity;
use Service\Resource\PoolService;

class Brasilia2 extends SpecificIntegration implements HasServicesInterface
{
    /** @var Client */
    private $client;

    const ASSEMBLY = "BrasiliaV2";

    const DOC_CEDULAEXTRANJERIA = "CE";
    const DOC_CEDULACIUDADANIA = "CC";
    const DOC_TARJETAIDENTIDAD = "TI";
    const DOC_PASAPORTE = "PS";

    const TOKEN_DURATION = 20;

    const TOKEN_POOL_SIZE = 25;
    const TOKEN_POOL_DELAY = 5000;
    const TOKEN_POOL_TIMEOUT = 40;

    const DEFAULT_TIMEOUT = 15;
    const DEFAULT_CONNECT_TIMEOUT = 5;

    const SEP_SEGMENTS = "::";

    private static $yCoords = [
        'IV' => 4,
        'IP' => 3,
        'DP' => 1,
        'DV' => 0,
    ];

    /** @var PoolService */
    private $poolService;

    /** @var Token */
    private $inventoryToken;

    public function __construct(Container $container)
    {
        parent::__construct($container);
        $this->client = $container['brasilia.client'];
        $this->poolService = $container['poolService'];
        //$this->inventoryToken = null;
        if ($inventoryToken !== null) {
            $this->inventoryToken = $inventoryToken;
        }
    }

    public function getOptions()
    {
        $options = new IntegrationOptions();
        $options->saveVSDPairs = true;
        return $options;
    }

    public static function ConfigureServices(Container $container)
    {
        $container['brasilia.client'] = function ($c) {
            /** @var SettingsBag $settings */
            $settings = $c['connectivitySettings'];
            $client = new GuzzleLogWrapper([
                'base_uri' => $settings->get('apiUrl'),
                'timeout' => $settings->getAsInt('clientTimeout', self::DEFAULT_TIMEOUT),
                'connect_timeout' => $settings->getAsInt('clientConnectTimeout', self::DEFAULT_CONNECT_TIMEOUT),
            ]);
            $logData = $c['logData'];
            $client->__setBooking($logData);
            return new Client($client);
        };
    }

    public function getTrips($sourceId, $destinationId, DateTimeInterface $doj)
    {
        $token = $this->getInventoryToken();
        try {
            $trips = $this->getValidTrips($token, $sourceId, $destinationId, $doj);
        } catch (Exception $e) {
            $token = $this->getInventoryToken();
            $trips = $this->getValidTrips($token, $sourceId, $destinationId, $doj);
        }
        return $trips;
    }

    private function getValidTrips($token, $sourceId, $destinationId, DateTimeInterface $doj)
    {
        $parameters = [
            'codOrigen' => $sourceId,
            'codDestino' => $destinationId,
            'fechaViaje' => $doj->format('d/m/Y'),
        ];
        $response = $this->client->getViajes($token, $parameters);
        $trips = $this->parseTripsResponse($response);
        return $this->filterTrips($trips, $sourceId, $destinationId);
    }

    private function parseTripsResponse($response)
    {
        if (!is_array($response)) {
            throw new Exception("invalid body received");
        }
        if (isset($response["error"])) {
            $msg = $response["message"] ?? "no error message";
            if ($this->isNoTripsError($msg)) {
                return [];
            }
            throw new Exception($msg);
        }
        return $response;
    }

    private function filterTrips($trips, $sourceId, $destinationId)
    {
        $supportConexiones = $this->settings->getAsBoolean('supportConexiones');
        $codEmpresa = $this->settings->get('codEmpresa');
        $validTrips = array_filter($trips, function ($trip) use ($codEmpresa, $supportConexiones, $sourceId, $destinationId) {
            return ($trip['isConexion'])
                ? ($supportConexiones && $this->isValidConnectedTrip($codEmpresa, $trip, $sourceId, $destinationId))
                : $this->isValidTrip($codEmpresa, $trip, $sourceId, $destinationId);
        });
        $sanitizedTrips = array_map([$this, 'sortTripComponents'], $validTrips);
        return array_values($sanitizedTrips);
    }

    private function isValidTrip($codEmpresa, $trip, $sourceId, $destinationId)
    {
        $lineas = $trip["lineas"];
        if (count($lineas) !== 1) {
            return false;
        }
        $linea = reset($lineas);
        return $sourceId == $linea['codigoOrigen']
            && $codEmpresa == $linea['codigoEmpresa']
            && $destinationId == $linea['codigoDestino'];
    }

    private function isValidConnectedTrip($codEmpresa, $trip, $sourceId, $destinationId)
    {
        $lineas = $trip["lineas"];
        if (count($lineas) !== 2) {
            return false;
        }
        list($linea1, $linea2) = $lineas;
        return $sourceId == $linea1['codigoOrigen']
            && $codEmpresa == $linea1['codigoEmpresa']
            && $codEmpresa == $linea2['codigoEmpresa']
            && $destinationId == $linea2['codigoDestino'];
    }

    private function sortTripComponents($trip)
    {
        if (count($trip['lineas']) === 1) {
            return $trip;
        }
        usort($trip['lineas'], function ($a, $b) {
            $depA = DateTimeImmutable::createFromFormat('d/m/Y H:i', $a['fechaHoraSalida']);
            $depB = DateTimeImmutable::createFromFormat('d/m/Y H:i', $b['fechaHoraSalida']);
            return strcmp($depA->format('Y-m-dH:i'), $depB->format('Y-m-dh:i'));
        });
        return $trip;
    }

    private function isNoTripsError($error)
    {
        return (preg_match("/No hay viajes disponibles/", $error) === 1);
    }

    public function getAuthorization()
    {
    }

    private function getInventoryToken()
    {
        if (isset($this->inventoryToken) && $this->inventoryToken !== null) {
       // if (!isset($this->inventoryToken)) {
            $this->inventoryToken = $this->getTokenFromPool();
        }
        return $this->inventoryToken->getToken();
    }

    private function getTokenFromPool()
    {
        $pool = $this->createTokenPool();
        $file = $pool->acquire();

        $token = new Token($file);
        if ($token->isExpired()) {
            $token->setToken($this->getNewToken());
        }

        return $token;
    }

    private function createTokenPool()
    {
        $size = $this->settings->getAsInt("TokenPoolSize", self::TOKEN_POOL_SIZE);
        $delay = $this->settings->getAsInt("TokenPoolDelay", self::TOKEN_POOL_DELAY);
        $timeout = $this->settings->getAsInt("TokenPoolTimeout", self::TOKEN_POOL_TIMEOUT);
        return $this->poolService->createPool(sprintf("%s-InventoryToken", self::ASSEMBLY), $size, $delay, $timeout);
    }

    private function getNewToken($whitelabel = "")
    {
        $parameters = [
            'username' => $this->settings->get("{$whitelabel}_username", $this->settings->get("username")),
            'password' => $this->settings->get("{$whitelabel}_password", $this->settings->get("password")),
        ];
        $token = $this->client->authenticate($parameters);
        return $token['token'];
    }

    public function getVendorRouteId($trip, $sourceId, $destinationId, $vendorId)
    {
        $vRouteIds = array_map(function ($linea) use ($vendorId) {
            return implode('-', [
                $vendorId,
                $linea['codigoOrigen'],
                $linea['codigoDestino'],
                $linea['nombreTipoServicio'],
                $linea['ruta'],
                $linea['hora'],
            ]);
        }, $trip['lineas']);
        return implode(self::SEP_SEGMENTS, $vRouteIds);
    }

    public function getListingSeatLayout($trip, $sourceId, $destinationId, DateTimeImmutable $doj)
    {
        $layouts = array_map([$this, 'buildTemplatedSeatlayout'], $trip['lineas']);
        if (count($layouts) === 1) {
            return reset($layouts);
        }
        return $this->combineSeatLayouts($layouts);
    }

    private function buildTemplatedSeatlayout($linea)
    {
        $availableSeats = $linea["sillasDisponibles"];
        $occupiedSeats = $linea["sillasOcupadas"];
        $totalSeats = $availableSeats + $occupiedSeats;

        $fare = Helper::getFareObject($linea['vlrTarifaDesc'], $this->getCurrency());
        $skip = $this->settings->getAsBoolean("TemplateSkipsSeats", false);

        $columns = intval(ceil($totalSeats / 4));

        $discounts = [];

        $num = 1;
        $map = [];
        for ($x = 0; $x < $columns; $x++) {
            $discounts["$num"] = 0;
            $available = ($num <= $availableSeats);
            $map[] = $this->makeTemplateSeat($num++, $x, 4, $fare, $available);
            $map[] = $this->makeTemplateSeat($num++, $x, 3, $fare, $available);
            $map[] = $this->makeTemplateSeat($num++, $x, 0, $fare, $available);
            $map[] = $this->makeTemplateSeat($num++, $x, 1, $fare, $available);
        }

        // Special case for missing seats
        if (is_numeric($variable) && $variable == 30) {
       // if ($totalSeats == 30 && $skip) {
            unset($map[28]);
            unset($map[29]);
        }

        $vSeats = array_values($map);
        $vSeats = array_slice($vSeats, 0, $totalSeats);

        return new SeatLayoutWrapper($vSeats, $availableSeats, $fare, [$discounts]);
    }

    private function makeTemplateSeat($num, $x, $y, $fare, $available)
    {
        $availabilityStatus = ($available) ? SeatAvailabilityStatus::AVAILABLE : SeatAvailabilityStatus::BOOKED;

        $vSeat = new VSeatStatus();
        $vSeat->setName($num);
        $vSeat->setNo($num);
        $vSeat->setParam('');
        $vSeat->setType(SeatType::SEATER);
        $vSeat->setSeatReservationType(SeatReservationType::NOT_RESERVED);
        $vSeat->setLength(1);
        $vSeat->setWidth(1);
        $vSeat->setXPos($x);
        $vSeat->setYPos($y);
        $vSeat->setZPos(1);
        $vSeat->setSeatAvailabiltyStatus($availabilityStatus);
        $vSeat->setFare($fare);
        return $vSeat;
    }

    public function getSeatLayout($trip, $sourceId, $destinationId, DateTimeImmutable $doj)
    {
        $layouts = array_map([$this, 'getIndividualSeatLayout'], $trip['lineas']);
        if (count($layouts) === 1) {
            return reset($layouts);
        }
        return $this->combineSeatLayouts($layouts);
    }

    private function parseSeatLayout($seatlayout, $busFare)
    {
        $available = 0;
        $discounts = [];

        $vSeats = [];
        foreach ($seatlayout as $seat) {
            $discounts["{$seat['numero']}"] = $seat['descuento'];

            list($x, $y) = $this->getSeatCoordinates($seat);

            $availabilityStatus = ($seat['estado'] === 'L')
                ? SeatAvailabilityStatus::AVAILABLE
                : SeatAvailabilityStatus::BOOKED;

            $fare = $this->getSeatFare($seat, $busFare);

            $vSeat = new VSeatStatus();
            $vSeat->setName($seat['numero']);
            $vSeat->setNo($seat['numero']);
            $vSeat->setParam('');
            $vSeat->setType(SeatType::SEATER);
            $vSeat->setSeatReservationType(SeatReservationType::NOT_RESERVED);
            $vSeat->setLength(1);
            $vSeat->setWidth(1);
            $vSeat->setXPos($x);
            $vSeat->setYPos($y);
            $vSeat->setZPos(1);
            $vSeat->setSeatAvailabiltyStatus($availabilityStatus);
            $vSeat->setFare($fare);
            $vSeats[] = $vSeat;
            $available += (int)$vSeat->isAvailable();
        }

        return new SeatLayoutWrapper($vSeats, $available, $vSeats[0]->getFare(), [$discounts]);
    }

    private function getIndividualSeatLayout($linea)
    {
        $token = $this->getInventoryToken();

        $fechaViajeRuta = $this->parseFechaViajeRuta($linea['fechaViajeRuta']);
        if ($fechaViajeRuta === false) {
            throw new SkippableBusException("unknown format for fechaViajeRuta");
        }

        $parameters = [
            'codOrigen' => $linea['codigoOrigen'],
            'codDestino' => $linea['codigoDestino'],
            'codEmpresa' => $linea['codigoEmpresa'],
            'fechaViaje' => $fechaViajeRuta->format("d/m/Y"),
            'ruta' => $linea['ruta'],
            'hora' => $linea['hora'],
            'adic' => $linea['adic'],
        ];
        $seatlayout = $this->client->getMapaSillas($token, $parameters);
        return $this->parseSeatLayout($seatlayout, $linea['vlrTarifaDesc']);
    }

    private function getSeatCoordinates($seat)
    {
        $u = $seat['ubicacion'];

        $x = substr($u, 2);
        $y = self::$yCoords[substr($u, 0, 2)];

        return [$x, $y];
    }

    private function getSeatFare($seat, $fare)
    {
        $amount = ((100 - $seat['descuento']) * $fare) / 100;
        $amount = intval(round($amount));
        $discount = $fare - $amount;

        return Helper::getFareObject(
            $amount,
            $this->getCurrency(),
            0,
            $discount
        );
    }

    /**
     * @param SeatLayoutWrapper[] $wrappers
     * @return SeatLayoutWrapper
     */
    private function combineSeatLayouts($wrappers)
    {
        $seats = [];
        foreach ($wrappers as $wrapper) {
            $layout = $wrapper->getVSeats();
            foreach ($layout as $item) {
                $seats[$item->getNo()] ??= [];
                $seats[$item->getNo()][] = $item;
            }
        }

        $extra = $this->combineLayoutExtraData($wrappers);
        $fare = $this->combineLayoutFares($wrappers);

        $mainWrapper = reset($wrappers);
        $mainLayout = $mainWrapper->getVSeats();

        foreach ($mainLayout as $seat) {
            if (!$seat->isSeat()) {
                continue;
            }
            $no = $seat->getNo();
            $seat->setFare($this->combineSeatFares($seats[$no]));
            $seat->setSeatAvailabiltyStatus($this->combineSeatAvailability($seats[$no]));
        }

        $availableSeats = array_reduce($mainLayout, function ($memo, $item) {
            return $memo + (int)$item->isAvailable();
        }, 0);

        return new SeatLayoutWrapper($mainLayout, $availableSeats, $fare, $extra);
    }

    /**
     * @param SeatLayoutWrapper[] $wrappers
     * @return array|null[]
     */
    private function combineLayoutExtraData($wrappers)
    {
        return array_map(function ($wrapper) {
            return $wrapper->getExtra()[0];
        }, $wrappers);
    }

    /**
     * @param SeatLayoutWrapper[] $wrappers
     * @return VendorFare
     */
    private function combineLayoutFares($wrappers)
    {
        $amt = 0;
        $net = 0;
        $tax = 0;
        $dsc = 0;
        foreach ($wrappers as $wrapper) {
            $f = $wrapper->getFare();
            $amt += $f->getAmount();
            $net += $f->getNetFare();
            $tax += $f->getTax();
            $dsc += $f->getVendorDiscount();
        }
        return Helper::getFareObject($net, $this->getCurrency(), $tax, $dsc);
    }

    /**
     * @param VSeatStatus[] $seats
     * @return VendorFare
     */
    private function combineSeatFares($seats)
    {
        $amt = 0;
        $net = 0;
        $tax = 0;
        $dsc = 0;
        foreach ($seats as $seat) {
            $f = $seat->getFare();
            $amt += $f->getAmount();
            $net += $f->getNetFare();
            $tax += $f->getTax();
            $dsc += $f->getVendorDiscount();
        }
        return Helper::getFareObject($net, $this->getCurrency(), $tax, $dsc);
    }

    /**
     * @param VSeatStatus[] $seats
     * @return string
     */
    private function combineSeatAvailability($seats)
    {
        $available = array_reduce($seats, function ($memo, $seat) {
            return $memo && $seat->isAvailable();
        }, true);
        return ($available) ? SeatAvailabilityStatus::AVAILABLE : SeatAvailabilityStatus::BOOKED;
    }

    public function getBpListArray($trip, $sourceId, $sourceName, $doj)
    {
        $date = DateTimeImmutable::createFromFormat('Y-m-d', $doj);

        $linea = reset($trip['lineas']);
        $time = $this->getFirstBoardingTime($trip, $doj);

        try {
            return Helper::getPoint(
                Helper::DROPPING_POINT,
                $linea['nombreOrigen'],
                $linea['codigoOrigen'],
                Helper::DateTimeImmutableToMinutes($time, $date),
                $linea['nombreOrigen']
            );
        } catch (Exception $e) {
            throw new SkippableBusException("Error parsing boarding point: {$e->getMessage()}", $e->getCode(), $e);
        }
    }
  
    public function getCityNames($vendorId, $trips, $sourceId, $destinationId):array
    {
        if (empty($trips)) {
            return [null, null];
        }
        $trip = $trips[0];
        return [$trip['nombreOrigen'], $trip['nombreDestino']];
    }

    public function getDpListArray($trip, $destinationId, $destinationName, DateTimeImmutable $depDate = null, $arrivalTime = null)
    {
        $linea = end($trip['lineas']);
        $time = DateTimeImmutable::createFromFormat('d/m/Y H:i', $linea['fechaHoraLlegada']);
        if (false === $time) {
            throw new SkippableBusException("invalid 'fechaHoraLlegada'");
        }

        try {
            return Helper::getPoint(
                Helper::DROPPING_POINT,
                $linea['nombreDestino'],
                $linea['codigoDestino'],
                Helper::DateTimeImmutableToMinutes($time, $depDate),
                $linea['nombreDestino']
            );
        } catch (Exception $e) {
            throw new SkippableBusException("Error parsing dropping point: {$e->getMessage()}", $e->getCode(), $e);
        }
    }

    public function getCurrency()
    {
        return "COP";
    }

    public function fillBusDetails(Bus $bus, $trip, $vendorId, $vRouteId, $sourceId, $destinationId, $dateOfJourney, $sourceName, $destinationName)
    {
        $linea = reset($trip['lineas']);
        $bus->setVendorServiceName($linea['nombreTipoServicio']);
        $bus->setVendorServiceId($this->getJourneyId($trip));
        $bus->setVendorBusTypeKey($linea['nombreTipoServicio']);
        $bus->setIsAutoEnabled(true);
        return $bus;
    }

    public function getParam42($trip)
    {
        $param42 = [];
        $param42 = $this->addParam42Via($trip, $param42);
        $param42 = $this->addLayoverData($trip, $param42);
        return (empty($param42)) ? "" : Utils::jsonEncode([$param42]);
    }

    private function addParam42Via($trip, $param42)
    {
        $linea = reset($trip['lineas']);
        if (!empty($linea["viaLinea"])) {
            $param42["via"] = $linea["viaLinea"];
        }
        return $param42;
    }

    private function addLayoverData($trip, $param42)
    {
        if (count($trip['lineas']) === 1) {
            return $param42;
        }

        list($linea1, $linea2) = $trip['lineas'];

        $departureTime = DateTimeImmutable::createFromFormat('d/m/Y H:i', $linea1['fechaHoraSalida']);
        $midPointArrivalTime = DateTimeImmutable::createFromFormat('d/m/Y H:i', $linea1['fechaHoraLlegada']);
        $midPointDepartureTime = DateTimeImmutable::createFromFormat('d/m/Y H:i', $linea2['fechaHoraSalida']);
        $arrivalTime = DateTimeImmutable::createFromFormat('d/m/Y H:i', $linea2['fechaHoraLlegada']);

        $departureMinutes = Helper::DateTimeImmutableToMinutes($departureTime, $departureTime);
        $midPointArrivalMinutes = Helper::DateTimeImmutableToMinutes($midPointArrivalTime, $departureTime);
        $midPointDepartureMinutes = Helper::DateTimeImmutableToMinutes($midPointDepartureTime, $departureTime);
        $arrivalMinutes = Helper::DateTimeImmutableToMinutes($arrivalTime, $departureTime);

        $duration1 = $midPointArrivalMinutes - $departureMinutes;
        $duration2 = $midPointDepartureMinutes - $midPointArrivalMinutes;
        $duration3 = $arrivalMinutes - $midPointDepartureMinutes;

        $layoverData = [
            "cityName" => $linea1['nombreDestino'],
            "bpName" => $linea2['nombreOrigen'],
            "droppingTime" => $midPointArrivalMinutes,
            "boardingTime" => $midPointDepartureMinutes,
            "journeyTime" => "$duration1:$duration2:$duration3"
        ];
        $param42['layoverData'] = $layoverData;
        return $param42;
    }

    public function getJourneyId($trip)
    {
        $ids = array_map(function ($linea) {
            return $linea['codigoLinea'];
        }, $trip['lineas']);
        return implode(self::SEP_SEGMENTS, $ids);
    }

    public function getClParams1($trip, $seatLayout)
    {
        $extras = $seatLayout->getExtra();
        $params = array_map(function ($extra) {
            return Utils::jsonEncode($extra);
        }, $extras);
        return implode(self::SEP_SEGMENTS, $params);
    }

    public function getClParams2($trip)
    {
        $params = array_map(function ($linea) {
            return implode("#", [
                $linea['vlrTarifaDesc'],
                $linea['vlrTarifa'],
                $linea['adic'],
            ]);
        }, $trip['lineas']);
        return implode(self::SEP_SEGMENTS, $params);
    }

    public function getClParams3($trip)
    {
        $params = array_map(function ($linea) {
            $fechaViajeRuta = $this->parseFechaViajeRuta($linea['fechaViajeRuta']);
            if ($fechaViajeRuta === false) {
                throw new SkippableBusException("unknown format for fechaViajeRuta");
            }
            return implode("#", [
                $linea['codigoEmpresa'],
                $linea['codigoOrigen'],
                $linea['codigoDestino'],
                $linea['ruta'],
                $fechaViajeRuta->format("d/m/Y"),
                $linea['hora'],
            ]);
        }, $trip['lineas']);
        return implode(self::SEP_SEGMENTS, $params);
    }

    public function getDocumentMap()
    {
        return [
            IdProofType::COLOMBIA_CEDULA_EXTRANJERIA => self::DOC_CEDULAEXTRANJERIA,
            IdProofType::COLOMBIA_CEDULA_CIUDADANIA => self::DOC_CEDULACIUDADANIA,
            IdProofType::COLOMBIA_TARJETA_IDENTIDAD => self::DOC_TARJETAIDENTIDAD,
            IdProofType::PASAPORTE => self::DOC_PASAPORTE,
        ];
    }

    public function blockSeats(IasTentativeBooking $request)
    {
        $wl = $request->whitelabel();
        $token = $this->getNewToken($wl);

        $seats = $request->seatNumbers();
        $passengers = $request->paxList();

        $cachekey = $request->transactionId();
        $cachedata = ['seats' => $seats, 'passengers' => $passengers, 'wl' => $wl, 'token' => $token];
        $this->cache->set($cachekey, $cachedata, $this->blockDuration());

        $parameterSets = $this->parseTentativeParameterSets($request);
        foreach ($parameterSets as $parameterSet) {
            $this->individualTripTentative($token, $parameterSet, $seats);
        }
        return $request->transactionId();
    }

    private function individualTripTentative($token, $parameters, $seats)
    {
        $blocked = [];
        try {
            foreach ($seats as $seat) {
                $parameters['silla'] = $seat;
                $res = $this->client->marcarSilla($token, $parameters);
                if ($res['error']) {
                    throw new VendorRequestException($res['message']);
                }
            }
        } catch (Exception $e) {
            throw new PartiallyBlockedSeatsException($blocked, implode("#", $blocked), $e->getMessage());
        }
    }

    private function parseTentativeParameterSets(IasTentativeBooking $tentative)
    {
        $params2 = explode(self::SEP_SEGMENTS, $tentative->clParam2());
        $params3 = explode(self::SEP_SEGMENTS, $tentative->clParam3());
        return array_map(function ($p2, $p3) {
            list($empresa, $origen, $destino, $ruta, $fecha, $hora) = explode('#', $p3);
            list(, , $adic) = explode('#', $p2);
            return [
                'codOrigen' => $origen,
                'codDestino' => $destino,
                'codEmpresa' => $empresa,
                'fechaViaje' => $fecha,
                'ruta' => $ruta,
                'hora' => $hora,
                'adic' => $adic,
            ];
        }, $params2, $params3);
    }

    public function extendBlockTime(IasExtendBooking $extend)
    {
        $cachekey = $extend->tempVPNR();
        $cachedata = $this->cache->get($cachekey, $this->blockDuration());

        if (false === $cachedata) {
            throw new Exception("Extension data was not found in cache");
        }

        $token = $cachedata['token'];
        $seats = $cachedata['seats'];
        $passengers = $cachedata['passengers'];

        $transactions = [];
        $journeys = $this->parseExtendJourneys($extend);
        $discountSets = $this->parseDiscounts($journeys, $passengers);
        $referenceIds = $this->makeReferenceIds($cachekey, count($journeys));

        foreach ($journeys as $i => $journey) {
            $tickets = $this->individualTripExtend($token, $journey, $passengers, $seats, $referenceIds[$i], $discountSets[$i]);
            $transactions[] = Util::array_pluck($tickets, "transactionID");
        }
        $this->cache->set("{$cachekey}-Transaction", $transactions, $this->extendedDuration());
    }

    private function parseDiscounts($journeys, $passengers)
    {
        list($totalFare, $journeyFares) = array_reduce($journeys, function ($memo, $journey) {
            list($vlrTarifaDesc, ,) = explode('#', $journey['clParam2']);
            $memo[0] += $vlrTarifaDesc;
            $memo[1][] = $vlrTarifaDesc;
            return $memo;
        }, [0, []]);

        list($fare1) = $journeyFares;
        $percentage1 = floatval($fare1) / $totalFare;

        $reddeals1 = [];
        $reddeals2 = [];
        foreach ($passengers as $i => $passenger) {
            list($offerId, $discount) = $this->makeDiscount($passenger);
            $discount1 = intval(round($percentage1 * $discount));
            $discount2 = $discount - $discount1;
            $reddeals1[] = [$offerId, $discount1];
            $reddeals2[] = [$offerId, $discount2];
        }
        return [$reddeals1, $reddeals2];
    }

    private function parseExtendJourneys(IasExtendBooking $extend)
    {
        $ids = explode(self::SEP_SEGMENTS, $extend->journeyId());
        $param1 = explode(self::SEP_SEGMENTS, $extend->clParam1());
        $param2 = explode(self::SEP_SEGMENTS, $extend->clParam2());
        $param3 = explode(self::SEP_SEGMENTS, $extend->clParam3());
        return array_map(function ($id, $p1, $p2, $p3) {
            return [
                'journeyId' => $id,
                'clParam1' => $p1,
                'clParam2' => $p2,
                'clParam3' => $p3,
            ];
        }, $ids, $param1, $param2, $param3);
    }

    private function individualTripExtend($token, $journey, $passengers, $seats, $referenceId, $discounts)
    {
        $tickets = $this->createTickets($token, $passengers, $seats, $journey, $referenceId, $discounts, true);
        foreach ($tickets as $ticket) {
            if ($ticket['error']) {
                throw new Exception("{$ticket['message']}");
            }
        }
        return $tickets;
    }

    public function unblockSeats(array $seatNumbers, DateTimeInterface $doj, IasExpireBooking $expire)
    {
        $cachekey = $expire->tempPNR();
        $cachedata = $this->cache->get($cachekey, $this->blockDuration());

        $token = $cachedata['token'];
        $seats = $expire->seatNumbers();

        $parameterSets = $this->parseExpireParameterSets($expire);

        $errors = [];
        foreach ($parameterSets as $parameterSet) {
            $e = $this->individualTripExpire($token, $parameterSet, $seats);
            $errors = array_merge($errors, $e);
        }

        if (!empty($errors)) {
            $blocked = array_keys($errors);
            $unblocked = array_diff($seats, $blocked);
            $msg = sprintf("Failed to unblock: %s", implode(", ", $blocked));
            throw new PartiallyUnblockedSeatsException($unblocked, $blocked, $errors, $msg);
        }
    }

    private function parseExpireParameterSets(IasExpireBooking $expire)
    {
        $param2 = explode(self::SEP_SEGMENTS, $expire->clParam2());
        $param3 = explode(self::SEP_SEGMENTS, $expire->clParam3());
        return array_map(function ($p2, $p3) {
            list($empresa, , , $ruta, $fecha, $hora) = explode('#', $p3);
            list(, , $adic) = explode('#', $p2);
            return [
                'codEmpresa' => $empresa,
                'fechaViaje' => $fecha,
                'ruta' => $ruta,
                'hora' => $hora,
                'adic' => $adic,
            ];
        }, $param2, $param3);
    }

    private function individualTripExpire($token, $parameters, $seats)
    {
        $errors = [];
        foreach ($seats as $seat) {
            $parameters['silla'] = $seat;
            try {
                $res = $this->client->desmarcarSilla($token, $parameters);
                if ($res['error']) {
                    $errors["$seat"] = $res['message'];
                }
            } catch (Exception $e) {
                $errors["$seat"] = $e->getMessage();
            }
        }
        return $errors;
    }

    public function confirmSale(IasConfirmBooking $confirm)
    {
        $wl = $confirm->whitelabel();

        $cachekey = "{$confirm->tempVPNR()}-Transaction";
        $transactions = $this->cache->get($cachekey, $this->extendedDuration());

        $seats = $confirm->seatNumbers();
        $passengers = $confirm->paxList();

        if (false === $transactions) {
            $cachekey = $confirm->tempVPNR();
            $cachedata = $this->cache->get($cachekey, $this->blockDuration());

            if (false === $cachedata) {
                throw new Exception("Tentative data was not found in cache");
            }

            $token = $cachedata['token'];
            $journeys = $this->parseConfirmJourneys($confirm);
            $discountSets = $this->parseDiscounts($journeys, $passengers);
            $referenceIds = $this->makeReferenceIds($cachekey, count($journeys));

            $ticketSets = [];
            foreach ($journeys as $i => $journey) {
                $ticketSets[] = $this->createTickets($token, $passengers, $seats, $journey, $referenceIds[$i], $discountSets[$i]);
            }
        } else {
            $token = $this->getNewToken($wl);
            $ticketSets = $this->confirmOfflineTickets($token, $transactions);
        }

        $errors = [];
        $result = new ConfirmResult();
        foreach ($seats as $i => $seat) {
            $tickets = Util::array_pluck($ticketSets, $i);
            $errors = $this->getTicketErrors($tickets);
            if (!empty($errors)) {
                $errors["$seat"] = implode(self::SEP_SEGMENTS, $errors);
                continue;
            }
            $numbers = Util::array_pluck($tickets, 'numeroTiquete');
            $seatpnr = implode(self::SEP_SEGMENTS, $numbers);
            $result->addConfirmedSeat($seat, $seatpnr, $seatpnr);
        }

        if (!empty($errors)) {
            $unconfirmed = array_keys($errors);
            $confirmed = $result->getConfirmedSeats();
            $msg = sprintf("Failed to confirm: %s", implode(", ", $unconfirmed));
            throw new PartiallyConfirmedSeatsException($confirmed, $unconfirmed, $msg);
        }

        return $result;
    }

    private function parseConfirmJourneys(IasConfirmBooking $confirm)
    {
        $ids = explode(self::SEP_SEGMENTS, $confirm->journeyId());
        $param1 = explode(self::SEP_SEGMENTS, $confirm->clParam1());
        $param2 = explode(self::SEP_SEGMENTS, $confirm->clParam2());
        $param3 = explode(self::SEP_SEGMENTS, $confirm->clParam3());
        return array_map(function ($id, $p1, $p2, $p3) {
            return [
                'journeyId' => $id,
                'clParam1' => $p1,
                'clParam2' => $p2,
                'clParam3' => $p3,
            ];
        }, $ids, $param1, $param2, $param3);
    }

    private function getTicketErrors($tickets)
    {
        $errors = [];
        foreach ($tickets as $ticket) {
            if ($ticket['error']) {
                $errors[] = $ticket['message'];
            }
        }
        return $errors;
    }

    private function createTickets($token, $passengers, $seats, $journey, $referenceId, $discounts, $offline = false)
    {
        $linea = $journey['journeyId'];
        list($empresa, $origen, $destino, $ruta, $fecha, $hora) = explode('#', $journey['clParam3']);
        list($vlrTarifaDesc, $vlrTarifa, $adic) = explode('#', $journey['clParam2']);
        $descuentos = json_decode($journey['clParam1'], true);

        $f = DateTimeImmutable::createFromFormat('d/m/Y', $fecha);
        $fechaViaje = $f->format('d/m/Y');
        $referenciaPago = intval(round(microtime(true) * 1000));
        $referenciaAliado = $referenceId;

        $pasajeros = $this->makePasajeros($passengers, $seats, $descuentos);

        $tickets = [];
        foreach ($passengers as $i => $passenger) {
            list($cupon, $valorCupon) = $discounts[$i];

            $request = [
                'codOrigen' => $origen,
                'codDestino' => $destino,
                'codigoLinea' => $linea,
                'codigoEmpresa' => $empresa,
                'fechaViaje' => $fechaViaje,
                'ruta' => $ruta,
                'hora' => $hora,
                'adic' => $adic,
                'pagoOffline' => $offline,
                'referenciaPago' => $referenciaPago,
                'referenciaAliado' => $referenciaAliado,
                'vlrTarifa' => $vlrTarifaDesc,
                'vlrTarifaBase' => $vlrTarifa,
                'pasajeros' => [$pasajeros[$i]],
            ];

            if ($valorCupon > 0) {
                $request['valorCupon'] = $valorCupon;
                $request['cuponDescto'] = $cupon;
                $request['vlrTarifa'] = $vlrTarifaDesc - $valorCupon;
                $request['vlrTarifaBase'] = $vlrTarifaDesc;
            }

            $ticket = $this->client->grabarTiquete($token, $request);
            $tickets[] = $ticket['tickets'][0];
        }

        return $tickets;
    }

    private function makePasajeros($passengers, $seats, $discounts)
    {
        return array_map(function ($passenger, $seat) use ($discounts) {
            $passenger['firstName'] = trim(preg_replace('/\s\s+/', ' ', $passenger['firstName']));
            $passenger['lastName'] = trim(preg_replace('/\s\s+/', ' ', $passenger['lastName']));
            $names = explode(" ", $passenger['firstName']);
            if (strlen($passenger['firstName']) + strlen($passenger['lastName']) >= 60) {
                $passenger['firstName'] = $names[0];
                $secondName = "";
                if (strlen($passenger['firstName']) + strlen($passenger['lastName']) >= 60) {
                    $passenger['lastName'] = substr($passenger['lastName'], 0, 39);
                }
                $surnames = explode(" ", $passenger['lastName']);
                $passenger['lastName'] = $surnames[0];
                $secondSurname = $surnames[1] ?? "";
            } else {
                $passenger['firstName'] = $names[0];
                $secondName = $names[1] ?? "";
                $surnames = explode(" ", $passenger['lastName']);
                $passenger['lastName'] = $surnames[0];
                $secondSurname = $surnames[1] ?? "";
            }
            $req = [
                'tipoIdentificacion' => $passenger['vendorIdProofType'],
                'identificacion' => $passenger['idProof'],
                'primerNombre' => substr($passenger['firstName'], 0, 20),
                'segundoNombre' => substr($secondName, 0, 20),
                'primerApellido' => substr($passenger['lastName'], 0, 20),
                'segundoApellido' => substr($secondSurname, 0, 20),
                'email' => $passenger['emailId'],
                'celular' => $passenger['mobileNo'],
                'silla' => $seat,
                'desctoSilla' => $discounts["$seat"],
            ];
            if (isset($passenger['dynProps']['dynProp']['Nationality']) && !empty($passenger['dynProps']['dynProp']['Nationality'])) {
                $req['nacionalidad'] = $passenger['dynProps']['dynProp']['Nationality'];
            }
            return $req;
        }, $passengers, $seats);
    }

    private function makeDiscount($passenger)
    {
        if (!isset($passenger['redDeals']) || empty($passenger['redDeals'])) {
            return ['', 0];
        }
        $reddeals = json_decode($passenger['redDeals'], true);
        $discount = intval(round($reddeals['DiscAmt'] / 100));
        $offer = strval($reddeals['OfferId']);
        return [$offer, $discount];
    }

    private function makeReferenceIds($transactionId, $count)
    {
        if ($count === 1) {
            return [$this->makeReferenceId($transactionId)];
        }
        $ids = [];
        for ($i = 1; $i <= $count; $i++) {
            $ids[] = sprintf("%s-%d", $this->makeReferenceId($transactionId), $i);
        }
        return $ids;
    }

    private function makeReferenceId($transactionId)
    {
        $transactionId = explode('|', $transactionId);
        $transactionId = explode("::", $transactionId[0]);
        $transactionId = substr($transactionId[1], 0, 8);
        return $transactionId . substr(uniqid(), 8, 4);
    }

    private function confirmOfflineTickets($token, $transactionSets)
    {
        // Temp Backwards Compat
        if (!is_array($transactionSets[0])) {
            $transactionSets = [$transactionSets];
        }
        // End Temp Backwards Compat
        $tickets = [];
        foreach ($transactionSets as $transactionSet) {
            $tickets[] = $this->individualTripOfflineConfirm($token, $transactionSet);
        }
        return $tickets;
    }

    private function individualTripOfflineConfirm($token, $transactions)
    {
        $transaction = reset($transactions);
        $parameters = ['transactionID' => $transaction];
        $ticket = $this->client->confirmarPagoOffLine($token, $parameters);
        return $ticket['tickets'];
    }

    public function cancelSeats(array $seatNumbers, DateTimeInterface $doj, IasCancelBooking $cancel)
    {
        throw new RuntimeException("Not supported");
    }

    public function getVSDPairs()
    {
        $token = $this->getNewToken();
        $sources = $this->client->getOrigenes($token);
        $vsds = [];
        foreach ($sources as $source) {
            $destionations = $this->client->getDestinos($token, $source['codigo']);
            foreach ($destionations as $dest) {
                if ($source['codigo'] === $dest['codigo']) {
                    continue;
                }
                $sCity = new VendorCity();
                $sCity->setId($source['codigo']);
                $sCity->setName($source['nombre']);
                $dCity = new VendorCity();
                $dCity->setId($dest['codigo']);
                $dCity->setName($dest['nombre']);
                $vPair = new VendorSDPair();
                $vPair->setVSource($sCity);
                $vPair->setVDestination($dCity);
                $vsds[] = $vPair;
            }
        }
        return $vsds;
    }

    function getFirstBoardingTime($trip, $doj)
    {
        $linea = reset($trip['lineas']);
        $time = DateTimeImmutable::createFromFormat('d/m/Y H:i', $linea['fechaHoraSalida']);
        if (false === $time) {
            throw new SkippableBusException("invalid 'fechaHoraSalida'");
        }
        return $time;
    }

    private function blockDuration()
    {
        return $this->settings->getAsInt('blockDuration');
    }

    private function extendedDuration()
    {
        return $this->settings->getAsInt('extendedDuration');
    }

    private function parseFechaViajeRuta($fechaViajeRuta)
    {
        if (strlen($fechaViajeRuta) == 10) {
            return DateTimeImmutable::createFromFormat('d/m/Y', $fechaViajeRuta);
        }
        return DateTimeImmutable::createFromFormat('d/m/y', $fechaViajeRuta);
    }
}
