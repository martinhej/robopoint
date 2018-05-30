<?php

namespace robopoint\Controller\WebContent;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

/**
 * Class IndexController.
 *
 * @package robopoint\Controller\WebContent
 */
class IndexController extends Controller
{
    /**
     * Provide basic landing page.
     *
     * @return \Symfony\Component\HttpFoundation\Response
     */
    public function index() {
        return $this->render('base.html.twig', array(
            'message' => 'robocloud - the space where robots chatter',
        ));
    }

}
